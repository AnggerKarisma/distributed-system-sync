# src/nodes/cache_manager.py

import logging
import asyncio
import aiohttp
import redis.asyncio as redis
import os
from enum import Enum
from collections import OrderedDict

REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))

class CacheState(Enum):
    MODIFIED = "M"    
    EXCLUSIVE = "E"   
    SHARED = "S"      
    INVALID = "I"     

class CacheManager:
    def __init__(self, node_id: str, peers: list[str], cache_capacity: int = 100):
        self.node_id = node_id
        self.peers = [p for p in peers if p != self.node_id]
        self.logger = logging.getLogger(f"CacheManager-{node_id.split(':')[-1]}")
        self.logger.info(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}") 

        redis_connection = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self.redis_client = redis_connection
        self.main_memory = redis_connection
        
        self.cache_data = OrderedDict()
        
        self.cache_states = {}
        self.cache_capacity = cache_capacity 
        
        self.http_session = None

    async def _get_http_session(self):
        if self.http_session is None or self.http_session.closed:
            self.http_session = aiohttp.ClientSession()
        return self.http_session

    async def _read_from_main_memory(self, key: str):
        self.logger.debug(f"Cache MISS. Reading '{key}' from Main Memory (Redis)...")
        return await self.main_memory.get(key)

    async def _write_to_main_memory(self, key: str, value: str):
        self.logger.debug(f"Writing '{key}' to Main Memory (Redis)...")
        await self.main_memory.set(key, value)
    async def read_data(self, key: str):
        """API Publik: Klien (misal, CPU) ingin membaca data."""
        
        state = self.cache_states.get(key)
        if state in (CacheState.MODIFIED, CacheState.EXCLUSIVE, CacheState.SHARED):
            self.logger.info(f"READ Hit: Key '{key}' state={state.value}. Mengambil dari cache lokal.")
            self.cache_data.move_to_end(key)
            return self.cache_data[key]
        self.logger.info(f"READ Miss: Key '{key}' state={state}.")
        snoop_responses = await self._broadcast_snoop_read(key)
        
        data_from_peer = None
        peer_had_modified = False
        
        for peer_data, peer_state in snoop_responses:
            if peer_state in (CacheState.MODIFIED, CacheState.EXCLUSIVE, CacheState.SHARED):
                data_from_peer = peer_data
                if peer_state == CacheState.MODIFIED:
                    peer_had_modified = True
        
        new_data = None
        new_state = None
        
        if data_from_peer:
            self.logger.info(f"Snoop Hit: Mendapat '{key}' dari peer.")
            new_data = data_from_peer
            new_state = CacheState.SHARED
        else:
            new_data = await self._read_from_main_memory(key)
            if new_data is None:
                return None # Tidak ada di mana pun
            
            self.logger.info(f"Snoop Miss: Mendapat '{key}' dari Main Memory.")
            new_state = CacheState.EXCLUSIVE             

        await self._cache_store(key, new_data, new_state)
        return new_data
    
    async def write_data(self, key: str, value: str):
        """API Publik: Klien (misal, CPU) ingin menulis data."""
        
        state = self.cache_states.get(key)
        
        if state == CacheState.MODIFIED or state == CacheState.EXCLUSIVE:

            self.logger.info(f"WRITE Hit: Key '{key}' state={state.value}. Menulis ke cache lokal.")
            await self._cache_store(key, value, CacheState.MODIFIED)
            return
            
        if state == CacheState.SHARED:
            self.logger.info(f"WRITE Hit: Key '{key}' state=SHARED. Mengirim Invalidate...")
            await self._broadcast_invalidate(key)
            await self._cache_store(key, value, CacheState.MODIFIED)
            return
        self.logger.info(f"WRITE Miss: Key '{key}'. Melakukan Read-For-Ownership...")
        await self._broadcast_invalidate(key) 
        await self._cache_store(key, value, CacheState.MODIFIED)

    async def _cache_store(self, key: str, value: str, state: CacheState):
        """Helper internal untuk menyimpan ke cache & mengurus LRU."""
        self.cache_data[key] = value
        self.cache_states[key] = state
        self.cache_data.move_to_end(key) 
        await self._evict_if_needed()

    async def _evict_if_needed(self):
        """Jika cache penuh, keluarkan item yg LRU (paling lama)."""
        while len(self.cache_data) > self.cache_capacity:
            evicted_key, evicted_value = self.cache_data.popitem(last=False)
            evicted_state = self.cache_states.pop(evicted_key)
            
            self.logger.warning(f"Cache penuh! Evicting '{evicted_key}' (State={evicted_state.value})...")
            
            if evicted_state == CacheState.MODIFIED:
                self.logger.warning(f"Write-Back: Menulis '{evicted_key}' ke Main Memory sebelum eviction.")
                await self._write_to_main_memory(evicted_key, evicted_value)

    async def _broadcast_snoop_read(self, key: str):
        """(Internal) Kirim snoop read ke semua peer."""
        session = await self._get_http_session()
        tasks = []
        for peer_url in self.peers:
            url = f"{peer_url}/cache/handle_snoop_read"
            tasks.append(session.post(url, json={'key': key}, timeout=0.5))
        
        results = []
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        for resp in responses:
            if isinstance(resp, aiohttp.ClientResponse) and resp.status == 200:
                data = await resp.json()
                state = CacheState(data['state']) if data['state'] else None
                results.append((data['data'], state))
            else:
                results.append((None, None)) 
        return results

    async def _broadcast_invalidate(self, key: str):
        """(Internal) Kirim invalidate ke semua peer."""
        session = await self._get_http_session()
        tasks = []
        for peer_url in self.peers:
            url = f"{peer_url}/cache/handle_invalidate"
            tasks.append(session.post(url, json={'key': key}, timeout=0.5))
        asyncio.gather(*tasks, return_exceptions=True)

    async def handle_snoop_read(self, key: str):
        """(Internal API) Peer lain bertanya apakah kita punya 'key'."""
        state = self.cache_states.get(key)
        
        if state == CacheState.MODIFIED:
            value = self.cache_data[key]
            await self._write_to_main_memory(key, value)
            self.cache_states[key] = CacheState.SHARED
            self.logger.info(f"Snoop HANDLER: '{key}' state M->S (Flush to memory)")
            return {'data': value, 'state': CacheState.SHARED.value}
            
        elif state == CacheState.EXCLUSIVE:
            self.cache_states[key] = CacheState.SHARED
            self.logger.info(f"Snoop HANDLER: '{key}' state E->S")
            return {'data': self.cache_data[key], 'state': CacheState.SHARED.value}

        elif state == CacheState.SHARED:
            self.logger.info(f"Snoop HANDLER: '{key}' state S. Sharing data.")
            return {'data': self.cache_data[key], 'state': CacheState.SHARED.value}
        
        else: 
            return {'data': None, 'state': None}

    async def handle_invalidate(self, key: str):
        """(Internal API) Peer lain memberitahu kita data 'key' tidak valid."""
        if self.cache_states.get(key):
            self.cache_states[key] = CacheState.INVALID
            self.logger.info(f"Invalidate HANDLER: State '{key}' -> INVALID")
        return {'success': True}