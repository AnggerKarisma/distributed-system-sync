import pytest
import pytest_asyncio
import httpx
import asyncio
import uuid

# Tandai semua tes di file ini sebagai asyncio
pytestmark = pytest.mark.asyncio

# Alamat node lokal kita
NODES = [
    "http://localhost:8081",
    "http://localhost:8082",
    "http://localhost:8083",
]

@pytest_asyncio.fixture
async def async_client():
    """Fixture untuk klien HTTP async."""
    async with httpx.AsyncClient() as client:
        yield client

async def get_leader_node(client: httpx.AsyncClient):
    """Helper untuk menemukan node mana yang sedang LEADER."""
    for node_url in NODES:
        try:
            resp = await client.get(f"{node_url}/raft/status")
            if resp.status_code == 200 and resp.json()['state'] == 'LEADER':
                return node_url
        except httpx.ConnectError:
            continue # Node mungkin down
    return None

async def test_raft_leader_election(async_client: httpx.AsyncClient):
    """Tes 1: Memastikan ada SATU leader yang terpilih."""
    leader = await get_leader_node(async_client)
    assert leader is not None, "Sistem gagal memilih leader!"

async def test_lock_acquire_and_release(async_client: httpx.AsyncClient):
    """Tes 2: Tes alur kerja Lock Manager (Acquire -> Release)."""
    leader = await get_leader_node(async_client)
    assert leader is not None, "Tidak ada leader untuk diajak bicara"
    
    lock_id = f"test_lock_{uuid.uuid4()}"
    client_id = "pytest_client"

    # 1. Acquire Lock
    resp_acq = await async_client.post(
        f"{leader}/lock/acquire",
        json={"lock_id": lock_id, "client_id": client_id, "type": "exclusive"},
        timeout=10.0
    )
    assert resp_acq.status_code == 200
    assert resp_acq.json()['status'] == 'acquired'

    # 2. Cek status di node FOLLOWER untuk melihat replikasi
    follower_url = next(n for n in NODES if n != leader)
    await asyncio.sleep(0.5)  # Beri waktu replikasi
    resp_stat = await async_client.get(f"{follower_url}/lock/status")
    assert resp_stat.status_code == 200
    assert lock_id in resp_stat.json()['locks']
    assert resp_stat.json()['locks'][lock_id]['owners'] == [client_id]

    # 3. Release Lock
    resp_rel = await async_client.post(
        f"{leader}/lock/release",
        json={"lock_id": lock_id, "client_id": client_id},
        timeout=10.0
    )
    assert resp_rel.status_code == 200
    assert resp_rel.json()['status'] == 'released'

async def test_cache_coherence_invalidation(async_client: httpx.AsyncClient):
    """Tes 3: Tes alur kerja Cache MESI (Write -> Invalidate)."""
    node1_url = NODES[0]
    node2_url = NODES[1]
    key = "cache_test_key"
    
    # 1. Tulis data ke memori
    await async_client.post(
        f"{node1_url}/cache/write_to_memory",
        json={"key": key, "value": "v1"}
    )

    # 2. Node 1 baca (State akan menjadi E - Exclusive)
    await async_client.post(f"{node1_url}/cache/read", json={"key": key})
    
    # 3. Node 2 baca (State Node1 -> S, Node2 -> S)
    await async_client.post(f"{node2_url}/cache/read", json={"key": key})
    
    resp1_pre = await async_client.get(f"{node1_url}/cache/status")
    resp2_pre = await async_client.get(f"{node2_url}/cache/status")
    assert resp1_pre.json()['cache_states'][key] == "S" 
    assert resp2_pre.json()['cache_states'][key] == "S"

    # 4. Node 1 MENULIS (Ini harus meng-invalidate Node 2)
    await async_client.post(
        f"{node1_url}/cache/write",
        json={"key": key, "value": "v2_new"}
    )
    
    # Beri waktu sedikit (0.1s) untuk invalidasi HTTP menyebar
    await asyncio.sleep(0.1)

    # 5. Cek status. Node1 -> M, Node2 -> I
    resp1_post = await async_client.get(f"{node1_url}/cache/status")
    resp2_post = await async_client.get(f"{node2_url}/cache/status")
    
    assert resp1_post.json()['cache_states'][key] == "M"
    assert resp2_post.json()['cache_states'][key] == "I"