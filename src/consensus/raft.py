# src/consensus/raft.py

import asyncio
import aiohttp
import random
import logging
import time
from enum import Enum
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class NodeState(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3

class RaftNode:
    def __init__(self, node_id, peers, state_machine_applier):
        self.node_id = node_id
        self.peers = [p for p in peers if p != self.node_id]
        self.logger = logging.getLogger(f"RaftNode-{self.node_id.split(':')[-1]}")
        
        # Callback untuk menerapkan log ke state machine (misal, ke LockManager)
        self.apply_to_state_machine = state_machine_applier

        # Raft Persistent State
        self.current_term = 0
        self.voted_for = None
        self.log = []  # List of (term, command) tuples

        # Raft Volatile State
        self.state = NodeState.FOLLOWER
        self.commit_index = -1
        self.last_applied = -1

        # Leader-specific State
        self.next_index = defaultdict(int)
        self.match_index = defaultdict(int)
        self.leader_id = None

        # Timers
        self.election_timeout = self._get_random_timeout()
        self.heartbeat_interval = 0.2 # Leader mengirim heartbeat setiap 500ms
        self.last_leader_contact = time.time()

    def _get_random_timeout(self):
        return random.uniform(3.0, 5.0) # 3.0 - 5.0 detik

    def _reset_election_timer(self):
        self.last_leader_contact = time.time()
        self.election_timeout = self._get_random_timeout()
        
    def get_last_log_index_term(self):
        if not self.log:
            return -1, 0
        return len(self.log) - 1, self.log[-1][0] # (last_index, last_term)

    async def run(self):
        """Main loop for the Raft node."""
        while True:
            try:
                if self.state == NodeState.FOLLOWER:
                    await self.run_follower()
                elif self.state == NodeState.CANDIDATE:
                    await self.run_candidate()
                elif self.state == NodeState.LEADER:
                    await self.run_leader()
            except Exception as e:
                self.logger.error(f"Error in main loop: {e}", exc_info=True)
                self.state = NodeState.FOLLOWER # Fallback ke follower
                await asyncio.sleep(1)

    async def run_follower(self):
        self.logger.debug(f"Term {self.current_term}: Running as FOLLOWER.")
        while self.state == NodeState.FOLLOWER:
            if time.time() - self.last_leader_contact > self.election_timeout:
                self.logger.info("Election timeout! Becoming candidate.")
                self.state = NodeState.CANDIDATE
                return
            await asyncio.sleep(0.1)

    async def run_candidate(self):
        self.current_term += 1
        self.voted_for = self.node_id
        self._reset_election_timer()
        self.logger.info(f"Term {self.current_term}: Running as CANDIDATE. Requesting votes...")
        
        votes_received = 1
        
        last_log_index, last_log_term = self.get_last_log_index_term()
        payload = {
            'term': self.current_term,
            'candidate_id': self.node_id,
            'last_log_index': last_log_index,
            'last_log_term': last_log_term
        }
        
        tasks = [self._send_rpc(peer, '/request_vote', payload) for peer in self.peers]
        for future in asyncio.as_completed(tasks):
            resp = await future
            if not resp:
                continue

            if resp['term'] > self.current_term:
                self.logger.info(f"Stepping down. Found higher term {resp['term']}.")
                self.state = NodeState.FOLLOWER
                self.current_term = resp['term']
                self.voted_for = None
                return

            if resp['vote_granted']:
                votes_received += 1
                if votes_received > (len(self.peers) + 1) // 2:
                    self.logger.info(f"Term {self.current_term}: Won election with {votes_received} votes.")
                    self.state = NodeState.LEADER
                    self._init_leader_state()
                    return
        
        # Jika loop selesai tanpa jadi leader (split vote atau timeout),
        # tetap jadi candidate dan timeout acak akan memulai election baru.
        self.logger.info("Election failed or split vote. Retrying.")


    async def run_leader(self):
        self.logger.info(f"Term {self.current_term}: Running as LEADER.")
        self.leader_id = self.node_id

        while self.state == NodeState.LEADER:
            # Kirim AppendEntries (heartbeat) ke semua peer
            tasks = [self._replicate_log_to_peer(peer) for peer in self.peers]
            await asyncio.gather(*tasks, return_exceptions=True)
            
            # Cek apakah kita bisa commit sesuatu
            await self._update_commit_index()
            
            await asyncio.sleep(self.heartbeat_interval)

    def _init_leader_state(self):
        """Panggil ini saat menjadi leader."""
        last_log_index, _ = self.get_last_log_index_term()
        self.next_index = defaultdict(lambda: last_log_index + 1)
        self.match_index = defaultdict(lambda: -1)

    async def _replicate_log_to_peer(self, peer):
        peer_next_index = self.next_index[peer]
        
        # Log Consistency Check
        # Jika next_index > 0, kita perlu prevLogIndex dan prevLogTerm
        if peer_next_index > 0:
            prev_log_index = peer_next_index - 1
            prev_log_term = self.log[prev_log_index][0]
        else:
            prev_log_index = -1
            prev_log_term = 0
            
        entries = self.log[peer_next_index:]
        
        payload = {
            'term': self.current_term,
            'leader_id': self.node_id,
            'prev_log_index': prev_log_index,
            'prev_log_term': prev_log_term,
            'entries': entries,
            'leader_commit': self.commit_index
        }
        
        resp = await self._send_rpc(peer, '/append_entries', payload)
        
        if not resp:
            self.logger.warning(f"No response from {peer} for AppendEntries.")
            return

        if resp['term'] > self.current_term:
            self.logger.info(f"Stepping down. Peer {peer} has higher term {resp['term']}.")
            self.state = NodeState.FOLLOWER
            self.current_term = resp['term']
            self.voted_for = None
            return

        if resp['success']:
            # Log berhasil direplikasi
            self.next_index[peer] = peer_next_index + len(entries)
            self.match_index[peer] = self.next_index[peer] - 1
        else:
            # Log Gagal (Log Inconsistency)
            # Mundurkan next_index dan coba lagi di heartbeat berikutnya
            self.next_index[peer] = max(0, self.next_index[peer] - 1)
            self.logger.warning(f"Log inconsistency for {peer}. Retrying with next_index={self.next_index[peer]}")

    async def _update_commit_index(self):
        """Cari index N tertinggi yang disetujui mayoritas."""
        last_log_index, _ = self.get_last_log_index_term()
        
        for N in range(last_log_index, self.commit_index, -1):
            # Cek apakah log[N] ada di term kita
            if self.log[N][0] == self.current_term:
                # Hitung berapa banyak follower yang punya log[N]
                match_count = 1 + sum(1 for peer in self.peers if self.match_index[peer] >= N)
                
                if match_count > (len(self.peers) + 1) // 2:
                    self.commit_index = N
                    self.logger.debug(f"Updated commit_index to {self.commit_index}")
                    break # Kita hanya update ke N tertinggi
        
        # Terapkan semua committed entries yang belum diterapkan
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            command = self.log[self.last_applied][1]
            try:
                self.apply_to_state_machine(command)
                self.logger.info(f"Applied log[{self.last_applied}]: {command}")
            except Exception as e:
                self.logger.error(f"Failed to apply log[{self.last_applied}]: {command}. Error: {e}")

    async def propose(self, command):
        """Dipanggil oleh klien (via LockManager) untuk mengusulkan perintah baru."""
        if self.state != NodeState.LEADER:
            return False, self.leader_id # Gagal, kembalikan info leader

        self.logger.info(f"Proposing new command: {command}")
        
        # 1. Tambahkan ke log lokal
        self.log.append((self.current_term, command))
        last_log_index, _ = self.get_last_log_index_term()
        
        # 2. Replikasi ke follower (ini akan terjadi di loop leader berikutnya,
        #    tapi kita bisa picu sekarang untuk latensi lebih rendah)
        tasks = [self._replicate_log_to_peer(peer) for peer in self.peers]
        await asyncio.gather(*tasks, return_exceptions=True)

        # 3. Tunggu sampai committed
        # Dalam implementasi nyata, ini bisa pakai future/event
        # Di sini kita polling sederhana
        start_time = time.time()
        while time.time() - start_time < 2.0: # Timeout 2 detik
            await self._update_commit_index()
            if self.commit_index >= last_log_index:
                self.logger.info(f"Proposal committed at index {last_log_index}.")
                return True, self.leader_id
            await asyncio.sleep(0.05)
            
        self.logger.error("Proposal failed to commit in time.")
        return False, self.leader_id

    # --- RPC Handlers (dipanggil oleh server API) ---

    async def handle_request_vote(self, data):
        term = data['term']
        candidate_id = data['candidate_id']
        candidate_last_log_index = data['last_log_index']
        candidate_last_log_term = data['last_log_term']

        if term < self.current_term:
            return {'term': self.current_term, 'vote_granted': False}

        if term > self.current_term:
            self.state = NodeState.FOLLOWER
            self.current_term = term
            self.voted_for = None
        
        my_last_log_index, my_last_log_term = self.get_last_log_index_term()
        
        # Cek kelengkapan log kandidat
        # (Log kandidat harus "up-to-date" atau lebih)
        log_ok = (candidate_last_log_term > my_last_log_term) or \
                 (candidate_last_log_term == my_last_log_term and \
                  candidate_last_log_index >= my_last_log_index)
        
        vote_granted = False
        if (self.voted_for is None or self.voted_for == candidate_id) and log_ok:
            self.voted_for = candidate_id
            vote_granted = True
            self._reset_election_timer() # Reset timer karena kita vote
            self.logger.info(f"Voted FOR {candidate_id} in term {self.current_term}")
        
        return {'term': self.current_term, 'vote_granted': vote_granted}

    async def handle_append_entries(self, data):
        term = data['term']
        leader_id = data['leader_id']
        
        if term < self.current_term:
            return {'term': self.current_term, 'success': False}

        if term >= self.current_term:
            if self.state != NodeState.FOLLOWER:
                self.logger.info(f"Stepping down. Received AppendEntries from new leader {leader_id}.")
            self.state = NodeState.FOLLOWER
            self.current_term = term
            self.voted_for = None
            
        self._reset_election_timer()
        self.leader_id = leader_id

        # Log Consistency Check
        prev_log_index = data['prev_log_index']
        prev_log_term = data['prev_log_term']

        if prev_log_index >= 0:
            if len(self.log) <= prev_log_index or self.log[prev_log_index][0] != prev_log_term:
                self.logger.warning("Log inconsistency detected!")
                return {'term': self.current_term, 'success': False}
        
        # Hapus log yang konflik (jika ada) dan tambahkan entri baru
        entries = data['entries']
        if entries:
            self.log = self.log[:prev_log_index + 1] + entries
            self.logger.info(f"Appended {len(entries)} new entries from leader.")

        # Update commit index
        leader_commit = data['leader_commit']
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)
            
            # Terapkan ke state machine
            while self.last_applied < self.commit_index:
                self.last_applied += 1
                command = self.log[self.last_applied][1]
                try:
                    self.apply_to_state_machine(command)
                    self.logger.info(f"Applied log[{self.last_applied}]: {command}")
                except Exception as e:
                    self.logger.error(f"Failed to apply log[{self.last_applied}]: {command}. Error: {e}")

        return {'term': self.current_term, 'success': True}

    # --- Helper Komunikasi ---
    async def _send_rpc(self, peer, endpoint, payload):
        url = f"{peer}{endpoint}"
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=0.5)) as session:
                async with session.post(url, json=payload) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    else:
                        self.logger.warning(f"RPC to {url} failed with status {resp.status}")
        except asyncio.TimeoutError:
            self.logger.warning(f"RPC to {url} timed out.")
        except aiohttp.ClientError as e:
            self.logger.warning(f"RPC to {url} failed: {e}")
        return None