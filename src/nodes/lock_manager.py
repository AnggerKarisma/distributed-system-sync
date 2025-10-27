# src/nodes/lock_manager.py

import logging
from collections import defaultdict
import asyncio
import time
from consensus.raft import NodeState

class LockManager:
    def __init__(self):
        # Ini adalah State Machine yang direplikasi
        # Format: self.locks[lock_id] = {'type': 'exclusive' | 'shared', 'owners': set(['client_a', 'client_b'])}
        self.locks = defaultdict(dict)
        
        # Ini TIDAK direplikasi. Ini hanya untuk request yang sedang menunggu di node INI.
        # Format: self.wait_queue[lock_id] = [asyncio.Event(), asyncio.Event()]
        self.wait_queue = defaultdict(list)
        
        self.logger = logging.getLogger("LockManager")
        self.raft_node = None # Akan di-set oleh main.py

    def set_raft_node(self, raft_node):
        self.raft_node = raft_node

    def apply_command(self, command):
        """
        Callback yang dipanggil oleh RaftNode SETELAH sebuah command di-commit.
        Logika di sini HARUS deterministik.
        """
        op = command['op']
        lock_id = command['lock_id']
        client_id = command['client_id']
        
        self.logger.info(f"Applying committed command: {command}")
        
        try:
            if op == 'acquire':
                lock_type = command['type']
                if lock_id not in self.locks:
                    # Kunci baru
                    self.locks[lock_id] = {'type': lock_type, 'owners': {client_id}}
                elif self.locks[lock_id]['type'] == 'shared' and lock_type == 'shared':
                    # Tambahkan shared owner
                    self.locks[lock_id]['owners'].add(client_id)
                else:
                    # Ini seharusnya tidak terjadi jika 'check_lock' bekerja,
                    # tapi ini adalah state machine, kita harus tangani.
                    self.logger.error(f"Failed to apply {command}: Lock conflict detected in state machine.")
                    
            elif op == 'release':
                if lock_id in self.locks and client_id in self.locks[lock_id]['owners']:
                    self.locks[lock_id]['owners'].remove(client_id)
                    if not self.locks[lock_id]['owners']:
                        # Jika tidak ada owner lagi, hapus kuncinya
                        del self.locks[lock_id]
                else:
                    self.logger.warning(f"Failed to apply {command}: Lock not held or client mismatch.")
                    
            # Setelah state berubah, "bangunkan" request yang mungkin menunggu
            self._notify_waiters(lock_id)

        except Exception as e:
            self.logger.error(f"Error applying command {command}: {e}", exc_info=True)

    def _notify_waiters(self, lock_id):
        """Membangunkan semua waiter lokal untuk lock_id ini."""
        if lock_id in self.wait_queue:
            waiters = self.wait_queue.pop(lock_id)
            for event in waiters:
                event.set() # Bangunkan coroutine yang sedang 'await event.wait()'

    def _check_lock_availability(self, lock_id, lock_type):
        """Memeriksa state LOKAL (terreplikasi) saat ini."""
        if lock_id not in self.locks:
            return True # Tersedia
        
        current_lock = self.locks[lock_id]
        if current_lock['type'] == 'exclusive':
            return False # Sedang dipegang eksklusif
            
        if current_lock['type'] == 'shared' and lock_type == 'exclusive':
            return False # Sedang dipegang shared, tidak bisa ambil eksklusif
            
        return True # (current=shared, request=shared) -> OK

    async def acquire_lock(self, lock_id, client_id, lock_type, timeout=5.0):
        """API untuk Klien: Mencoba mendapatkan kunci."""
        if not self.raft_node or self.raft_node.state != NodeState.LEADER:
            return {'success': False, 'error': 'Not a leader', 'leader_id': self.raft_node.leader_id}
            
        self.logger.info(f"Client {client_id} attempting to acquire {lock_type} lock on {lock_id}")
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            # 1. Cek state lokal
            if self._check_lock_availability(lock_id, lock_type):
                # 2. Jika tersedia, ajukan (propose) ke Raft
                command = {
                    'op': 'acquire',
                    'lock_id': lock_id,
                    'client_id': client_id,
                    'type': lock_type
                }
                success, _ = await self.raft_node.propose(command)
                
                if success:
                    self.logger.info(f"Client {client_id} successfully acquired {lock_id}")
                    return {'success': True, 'lock_id': lock_id, 'status': 'acquired'}
                else:
                    # Gagal propose (misal, leadership hilang)
                    return {'success': False, 'error': 'Failed to commit proposal', 'leader_id': self.raft_node.leader_id}

            # 3. Jika tidak tersedia, tunggu
            wait_event = asyncio.Event()
            self.wait_queue[lock_id].append(wait_event)
            
            try:
                # Tunggu notifikasi ATAU timeout
                remaining_time = timeout - (time.time() - start_time)
                await asyncio.wait_for(wait_event.wait(), timeout=max(0.1, remaining_time))
            except asyncio.TimeoutError:
                self.logger.warning(f"Client {client_id} timed out waiting for {lock_id}")
                # Hapus event dari antrian jika masih ada
                if lock_id in self.wait_queue and wait_event in self.wait_queue[lock_id]:
                    self.wait_queue[lock_id].remove(wait_event)
                return {'success': False, 'status': 'timeout'}
            
            # Setelah "bangun", loop akan berulang dan cek lagi
            self.logger.debug(f"Client {client_id} re-checking lock {lock_id} after wake up.")
            
        return {'success': False, 'status': 'timeout'}


    async def release_lock(self, lock_id, client_id):
        """API untuk Klien: Mencoba melepas kunci."""
        if not self.raft_node or self.raft_node.state != NodeState.LEADER:
            return {'success': False, 'error': 'Not a leader', 'leader_id': self.raft_node.leader_id}

        # Cek cepat (optimasi): jika kunci tidak ada, jangan propose
        if lock_id not in self.locks or client_id not in self.locks[lock_id]['owners']:
             return {'success': False, 'error': 'Lock not held by this client'}
             
        command = {
            'op': 'release',
            'lock_id': lock_id,
            'client_id': client_id
        }
        
        success, _ = await self.raft_node.propose(command)
        
        if success:
            self.logger.info(f"Client {client_id} successfully released {lock_id}")
            return {'success': True, 'lock_id': lock_id, 'status': 'released'}
        else:
            return {'success': False, 'error': 'Failed to commit proposal', 'leader_id': self.raft_node.leader_id}