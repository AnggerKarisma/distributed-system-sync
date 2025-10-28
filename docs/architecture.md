# Dokumentasi Arsitektur Sistem Sinkronisasi Terdistribusi

Dokumen ini merinci arsitektur teknis dan algoritma inti yang digunakan dalam sistem sinkronisasi terdistribusi ini.

## A.1 Arsitektur Sistem

Sistem ini dirancang menggunakan arsitektur *microservice* yang di-container-isasi, terdiri dari beberapa komponen utama yang bekerja secara independen namun terkoordinasi.

### Komponen Utama

1.  **Node Aplikasi (Python/FastAPI):**
    * Ini adalah unit kerja utama sistem. Kita menjalankan 3 *instance* identik dari aplikasi ini (`node1`, `node2`, `node3`).
    * Setiap node memiliki *tiga peran* sekaligus:
        1.  **Anggota Raft:** Berpartisipasi dalam konsensus untuk *Distributed Lock Manager*.
        2.  **Partisi Queue:** Bertanggung jawab atas *shard* antrian berdasarkan *Consistent Hashing*.
        3.  **Peer Cache:** Bertindak sebagai *cache L1* yang koheren menggunakan protokol MESI.
    * Semua komunikasi antar-node (Raft, forwarding, snooping) terjadi melalui HTTP (FastAPI endpoint internal).

2.  **Redis (State & Persistence):**
    * Redis bertindak sebagai komponen pendukung utama.
    * **Sebagai Main Memory:** Digunakan oleh *Cache Manager* (MESI) sebagai "RAM" atau sumber kebenaran data utama.
    * **Sebagai Persistence Layer:** Digunakan oleh *Queue Manager* untuk menyimpan pesan antrian secara persisten (memenuhi Requirement B.3).

3.  **Klien (Client):**
    * Pengguna eksternal (misalnya, `curl`, aplikasi lain) yang berinteraksi dengan sistem melalui API publik FastAPI (cth: `/lock/acquire`, `/queue/push`).

4.  **Jaringan (Docker Compose):**
    * Semua service (3 node + 1 Redis) dijalankan dalam satu jaringan virtual (`distributed_net`) yang disediakan oleh Docker Compose. Ini memungkinkan mereka untuk saling menemukan satu sama lain menggunakan nama service (cth: `http://node1:8080`, `redis:6379`).

---

## A.2 Penjelasan Algoritma

### 1. Distributed Lock Manager (Raft Consensus)

Lock Manager diimplementasikan sebagai *Replicated State Machine* (RSM) yang dijamin konsistensinya oleh algoritma Raft.

* **Logika Inti:** State (daftar kunci yang sedang dipegang) disimpan di `lock_manager.py`. Modul `raft.py` digunakan untuk menyetujui *setiap perubahan* pada state tersebut.
* **Leader Election:** Node berada dalam 3 status: `Follower`, `Candidate`, atau `Leader`. Jika `Follower` tidak mendengar *heartbeat* dari `Leader` dalam `election_timeout`, ia menjadi `Candidate` dan memulai pemilihan. `Leader` dipilih berdasarkan suara mayoritas (N/2 + 1).
* **Log Replication (State Machine):**
    1.  Klien mengirim `POST /lock/acquire` ke `Leader`.
    2.  `Leader` *tidak* langsung memberikan kunci. Ia membuat *log entry* (perintah) dan mereplikasinya ke `Follower`.
    3.  Setelah mayoritas `Follower` mengkonfirmasi penerimaan log, `Leader` akan *commit* log tersebut.
    4.  Baru setelah *commit*, `Leader` (dan semua `Follower`) menerapkan perintah itu ke *state machine* lokal mereka (`lock_manager.apply_command`).
    5.  Hanya `Leader` yang dapat memberikan kunci (shared/exclusive), menjamin konsistensi linier.

### 2. Distributed Queue System (Consistent Hashing)

Sistem antrian mempartisi data (antrian) di seluruh node yang ada menggunakan *Consistent Hashing* untuk meminimalkan pergerakan data saat node ditambahkan/dihapus.

* **Hash Ring:** Kami menggunakan `hashlib.md5` untuk memetakan nama antrian (cth: "antrian_A") dan *virtual nodes* (cth: "node1:1", "node1:2", ...) ke sebuah "cincin" hash numerik.
* **Kepemilikan Data:** Sebuah antrian "dimiliki" oleh node virtual pertama yang ditemui searah jarum jam dari hash antrian tersebut di dalam cincin.
* **Request Forwarding:**
    1.  Klien mengirim `POST /queue/push` ke node mana pun (misal, `node1`).
    2.  `node1` menghitung hash dari `queue_name` dan mencari pemiliknya di *hash ring*.
    3.  Jika pemiliknya adalah `node3`, `node1` akan *meneruskan (forward)* request tersebut ke `node3` melalui endpoint internal (`POST /queue/internal_push`).
    4.  `node3` menerima request *forwarded* dan mengeksekusinya (menyimpan ke Redis).
* **Persistence:** Redis (`RPUSH`, `BLPOP`) digunakan sebagai *backend* penyimpanan yang persisten untuk semua antrian.

### 3. Distributed Cache Coherence (Protokol MESI)

Setiap node memelihara *cache* lokal (L1) untuk data. Protokol MESI (Modified, Exclusive, Shared, Invalid) disimulasikan di level aplikasi untuk menjaga agar semua *cache* L1 ini tetap koheren.

* **Komponen:**
    * **Cache Lokal (L1):** Sebuah `OrderedDict` di `cache_manager.py` (untuk LRU).
    * **Main Memory (RAM):** Service Redis.
* **Status MESI:**
    * **M (Modified):** Data di cache lokal telah diubah, berbeda dengan Main Memory. Ini adalah satu-satunya salinan yang valid.
    * **E (Exclusive):** Data di cache lokal sama dengan Main Memory. Tidak ada node lain yang memiliki salinan ini.
    * **S (Shared):** Data di cache lokal sama dengan Main Memory. Node lain *mungkin* memiliki salinan ini.
    * **I (Invalid):** Data di cache ini tidak valid (kedaluwarsa).
* **Skenario Komunikasi (Snooping):**
    1.  **Node1 Read (PrRd) 'x':** `Node1` mengalami *Cache Miss*. Ia mengirim "snoop read" (HTTP POST ke `/cache/handle_snoop_read`) ke `node2` dan `node3`.
        * **Kasus A (Snoop Miss):** Tak ada yang punya 'x'. `Node1` mengambil data dari Redis. State menjadi **E (Exclusive)**.
        * **Kasus B (Snoop Hit):** `Node2` punya 'x' (state E/S/M). `Node2` mengirimkan data ke `Node1` dan mengubah state-nya sendiri menjadi **S (Shared)**. `Node1` menyimpan data dan state-nya menjadi **S (Shared)**.
    2.  **Node1 Write (PrWr) 'x':**
        * **Kasus A (State E/M):** `Node1` memiliki hak tulis. Ia langsung menulis ke *cache* lokalnya. State menjadi **M (Modified)**.
        * **Kasus B (State S):** `Node1` tidak memiliki hak tulis eksklusif. Ia mengirim "invalidate" (HTTP POST ke `/cache/handle_invalidate`) ke `node2` dan `node3`. `Node2` dan `Node3` mengubah state mereka menjadi **I (Invalid)**. `Node1` kemudian menulis ke *cache* lokalnya dan state-nya menjadi **M (Modified)**.
* **LRU & Write-Back:** Jika *cache* penuh, item *Least Recently Used* (LRU) akan di-*evict*. Jika state item tersebut adalah **M (Modified)**, data akan di-*write-back* (ditulis) ke Redis sebelum dihapus.