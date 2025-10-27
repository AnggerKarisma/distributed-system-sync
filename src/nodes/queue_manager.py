# src/nodes/queue_manager.py

import logging
import aiohttp
import redis.asyncio as redis
import os
from utils.consistent_hashing import ConsistentHashing

REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))

class QueueManager:
    def __init__(self, node_id: str, hash_ring: ConsistentHashing):
        self.node_id = node_id
        self.hash_ring = hash_ring
        self.logger = logging.getLogger(f"QueueManager-{node_id.split(':')[-1]}")
        self.logger.info(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}")
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

        # Sesi HTTP untuk forwarding request ke node lain
        self.http_session = None

    async def _get_http_session(self):
        """Lazy-load http session."""
        if self.http_session is None:
            self.http_session = aiohttp.ClientSession()
        return self.http_session

    # --- Logika Inti ---

    async def push_message(self, queue_name: str, message: str):
        """
        API untuk Klien: Mendorong pesan ke antrian.
        Menangani logika forwarding.
        """
        responsible_node = self.hash_ring.get_node(queue_name)
        self.logger.info(f"Queue '{queue_name}' -> mapped to node '{responsible_node}'")

        if responsible_node == self.node_id:
            # Jika node ini yang bertanggung jawab, dorong ke Redis
            return await self._internal_push(queue_name, message)
        else:
            # Jika bukan, teruskan (forward) request ke node yg benar
            return await self._forward_push_request(responsible_node, queue_name, message)

    async def pop_message(self, queue_name: str, timeout: int = 5):
        """
        API untuk Klien: Mengambil pesan dari antrian.
        Menangani logika forwarding.
        """
        responsible_node = self.hash_ring.get_node(queue_name)
        self.logger.info(f"Queue '{queue_name}' -> mapped to node '{responsible_node}'")

        if responsible_node == self.node_id:
            # Node ini yg bertanggung jawab, ambil dari Redis
            return await self._internal_pop(queue_name, timeout)
        else:
            # Bukan node ini, teruskan (forward) request
            return await self._forward_pop_request(responsible_node, queue_name, timeout)

    # --- Logika Internal (Eksekusi Sebenarnya) ---

    async def _internal_push(self, queue_name: str, message: str):
        """Mendorong pesan ke Redis (Requirement B.3: Persistence)."""
        try:
            await self.redis_client.rpush(queue_name, message)
            return {"success": True, "action": "pushed", "responsible_node": self.node_id}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def _internal_pop(self, queue_name: str, timeout: int):
        """Mengambil pesan dari Redis (Blocking Pop)."""
        try:
            # BLPOP adalah blocking pop, sempurna untuk consumer
            # Ini memenuhi Requirement B.5 (At-least-once, jika klien ACK)
            message = await self.redis_client.blpop(queue_name, timeout=timeout)
            if message:
                # blpop mengembalikan tuple (nama_antrian, pesan)
                return {"success": True, "action": "popped", "message": message[1]}
            else:
                return {"success": True, "action": "popped", "message": None} # Timeout
        except Exception as e:
            return {"success": False, "error": str(e)}

    # --- Logika Forwarding (Simulasi Distribusi) ---

    async def _forward_push_request(self, node_url: str, queue_name: str, message: str):
        session = await self._get_http_session()
        target_url = f"{node_url}/queue/internal_push" # Endpoint internal baru
        payload = {"queue_name": queue_name, "message": message}
        
        self.logger.warning(f"Forwarding PUSH for '{queue_name}' to {target_url}")
        try:
            async with session.post(target_url, json=payload, timeout=2.0) as resp:
                return await resp.json()
        except Exception as e:
            self.logger.error(f"Failed to forward request to {target_url}: {e}")
            return {"success": False, "error": f"Forwarding failed: {e}"}

    async def _forward_pop_request(self, node_url: str, queue_name: str, timeout: int):
        session = await self._get_http_session()
        target_url = f"{node_url}/queue/internal_pop" # Endpoint internal baru
        payload = {"queue_name": queue_name, "timeout": timeout}

        self.logger.warning(f"Forwarding POP for '{queue_name}' to {target_url}")
        try:
            # Timeout client harus lebih lama dari timeout blpop
            async with session.post(target_url, json=payload, timeout=timeout + 2.0) as resp:
                return await resp.json()
        except Exception as e:
            self.logger.error(f"Failed to forward request to {target_url}: {e}")
            return {"success": False, "error": f"Forwarding failed: {e}"}