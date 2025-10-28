import random
from locust import HttpUser, task, between, events

# --- Skenario 1: Lock Manager ---

class LockUser(HttpUser):
    wait_time = between(0.1, 0.5) # Waktu tunggu antar task
    
    def on_start(self):
        """Dipanggil saat 'user' virtual ini mulai."""
        self.client_id = f"locust_user_{self.environment.runner.user_count}"
        self.leader_url = "" # Kita cari leader dulu
        self.find_leader()

    def find_leader(self):
        """Mencari leader Raft. Locust akan 'host-less', jadi kita tentukan host."""
        node_urls = ["http://localhost:8081", "http://localhost:8082", "http://localhost:8083"]
        for url in node_urls:
            try:
                resp = self.client.get(f"{url}/raft/status")
                if resp.ok and resp.json()['state'] == 'LEADER':
                    # self.client.host hanya bisa di-set sekali
                    if not self.leader_url: 
                        # 'host' dari user ini akan di-set ke leader
                        self.client.host = url 
                        self.leader_url = url
                        print(f"User {self.client_id} menemukan leader di {url}")
                    return
            except Exception:
                continue
        print("GAGAL MENEMUKAN LEADER, tes akan gagal")

    @task(5) # Bobot 5: Sering-sering tes ini
    def acquire_and_release_shared_lock(self):
        """Skenario: Dapatkan 1 dari 10 lock 'shared' yg berbeda."""
        lock_id = f"shared_lock_{random.randint(1, 10)}"
        
        # Acquire
        self.client.post(
            "/lock/acquire",
            json={"lock_id": lock_id, "client_id": self.client_id, "type": "shared"},
            name="/lock/acquire (shared)"
        )
        
        # Release (asumsikan kita langsung lepas)
        self.client.post(
            "/lock/release",
            json={"lock_id": lock_id, "client_id": self.client_id},
            name="/lock/release (shared)"
        )

    @task(1) # Bobot 1: Jarang-jarang tes ini
    def high_contention_exclusive_lock(self):
        """Skenario (B.1): Kontensi tinggi pada SATU lock eksklusif."""
        lock_id = "THE_ONE_LOCK" # Semua user memperebutkan ini
        
        resp = self.client.post(
            "/lock/acquire",
            json={"lock_id": lock_id, "client_id": self.client_id, "type": "exclusive"},
            name="/lock/acquire (exclusive-contention)"
        )
        
        if resp.ok:
            # Jika berhasil, kita lepas lagi
            self.client.post(
                "/lock/release",
                json={"lock_id": lock_id, "client_id": self.client_id},
                name="/lock/release (exclusive-contention)"
            )

# --- Skenario 2: Queue Manager ---

class QueueUser(HttpUser):
    wait_time = between(0.1, 1.0)
    
    # Kita tidak perlu leader. Request bisa ke node manapun (akan di-forward)
    # Kita akan set host saat menjalankan locust.
    
    @task(3)
    def push_to_queue(self):
        """Skenario (B.1): Produser mengirim pesan."""
        queue_name = f"queue_{random.randint(1, 5)}"
        self.client.post(
            "/queue/push",
            json={"queue_name": queue_name, "message": f"msg_for_{self.environment.runner.user_count}"},
            name="/queue/push"
        )
        
    @task(1)
    def pop_from_queue(self):
        """Skenario (B.1): Konsumer mengambil pesan."""
        queue_name = f"queue_{random.randint(1, 5)}"
        self.client.post(
            "/queue/pop",
            json={"queue_name": queue_name, "timeout": 1},
            name="/queue/pop"
        )

class CacheUser(HttpUser):
    wait_time = between(0.01, 0.1) 
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key_pool = [f"hotkey_{i}" for i in range(5)]
        self.value_counter = 0

    @task(10) # Bobot 10: Lebih banyak membaca
    def read_from_cache_pool(self):
        """
        Skenario: Membaca data dari 'hot-spot' pool.
        Ini akan sering menyebabkan state E -> S atau S -> S.
        """
        key = random.choice(self.key_pool)
        
        self.client.post(
            "/cache/read",
            json={"key": key},
            name="/cache/read (Hot Pool)"
        )

    @task(2) # Bobot 2: Sesekali menulis
    def write_to_cache_pool(self):
        """
        Skenario: Menulis data ke 'hot-spot' pool.
        Ini AKAN memicu invalidation (transisi S -> M).
        Ini adalah tes performa yang sebenarnya.
        """
        key = random.choice(self.key_pool)
        self.value_counter += 1
        value = f"new_val_{self.value_counter}"
        
        self.client.post(
            "/cache/write",
            json={"key": key, "value": value},
            name="/cache/write (Invalidation)"
        )