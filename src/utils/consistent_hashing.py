import hashlib
import bisect
from typing import List

class ConsistentHashing:
    def __init__(self, nodes: List[str], replicas: int = 10):
        """
        Inisialisasi cincin (ring) hash.
        :param nodes: List node IDs, cth: ['http://localhost:8081', ...]
        :param replicas: Jumlah replika virtual per node untuk distribusi yg lebih baik.
        """
        self.replicas = replicas
        self.ring = [] # List berisi (hash, node_id)
        self.ring_hashes = [] # List berisi hash saja (untuk bisect)
        
        for node in nodes:
            self.add_node(node)
            
    def _hash_key(self, key: str) -> int:
        """Mengubah string key menjadi integer hash."""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node(self, node: str):
        """Menambahkan node (dan replikanya) ke cincin."""
        for i in range(self.replicas):
            key = f"{node}:{i}"
            h = self._hash_key(key)
            bisect.insort(self.ring, (h, node))
        self.ring_hashes = [h for h, n in self.ring]

    def remove_node(self, node: str):
        """Menghapus node (dan replikanya) dari cincin."""
        self.ring = [(h, n) for h, n in self.ring if n != node]
        self.ring_hashes = [h for h, n in self.ring]

    def get_node(self, key: str) -> str:
        """
        Mendapatkan node yang bertanggung jawab untuk key (nama antrian) ini.
        """
        if not self.ring:
            return None
            
        h = self._hash_key(key)
        
        # Cari posisi hash key di cincin
        # bisect_right menemukan "insertion point" di kanan
        idx = bisect.bisect_right(self.ring_hashes, h)
        
        if idx == len(self.ring_hashes):
            idx = 0 # Wrap-around (kembali ke awal cincin)
            
        return self.ring[idx][1] # Kembalikan node_id