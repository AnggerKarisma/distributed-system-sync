import pytest
from src.utils.consistent_hashing import ConsistentHashing

@pytest.fixture
def hash_ring():
    """Membuat hash ring standar untuk semua tes."""
    nodes = ['http://node1', 'http://node2', 'http://node3']
    return ConsistentHashing(nodes, replicas=5) # 5 replika untuk tes

def test_determinism(hash_ring):
    """
    Tes Kunci: Memastikan key yang sama SELALU dipetakan ke node yang sama.
    """
    key = "antrian_A"
    node1 = hash_ring.get_node(key)
    node2 = hash_ring.get_node(key)
    assert node1 == node2

def test_distribution(hash_ring):
    """
    Tes Sederhana: Memastikan 100 key didistribusikan, tidak semua ke satu node.
    """
    keys = [f"key_{i}" for i in range(100)]
    node_assignments = {
        'http://node1': 0,
        'http://node2': 0,
        'http://node3': 0,
    }
    
    for key in keys:
        node = hash_ring.get_node(key)
        node_assignments[node] += 1
    
    print(node_assignments)
    # Memastikan tidak ada node yang mendapat 0 key
    assert node_assignments['http://node1'] > 0
    assert node_assignments['http://node2'] > 0
    assert node_assignments['http://node3'] > 0
    # Memastikan tidak ada 1 node yang mengambil > 80%
    assert node_assignments['http://node1'] < 80

def test_node_removal_impact(hash_ring):
    """
    Tes Kunci Consistent Hashing:
    Saat 1 node dihapus, hanya key dari node itu yang pindah.
    """
    keys = [f"key_{i}" for i in range(100)]
    
    # 1. Petakan semua key sebelum node dihapus
    initial_map = {key: hash_ring.get_node(key) for key in keys}
    
    # 2. Hapus node
    hash_ring.remove_node('http://node2')
    
    # 3. Petakan lagi
    moved_keys = 0
    for key in keys:
        new_node = hash_ring.get_node(key)
        
        if initial_map[key] != new_node:
            # Key ini pindah
            moved_keys += 1
            # Pastikan key TIDAK pindah ke node2 (yg sudah dihapus)
            assert new_node != 'http://node2'
            # Pastikan key yang pindah HANYA yang dari node2
            assert initial_map[key] == 'http://node2'
        else:
            # Key ini tidak pindah
            assert initial_map[key] != 'http://node2'
            
    print(f"Total keys moved after removing node2: {moved_keys}")
    assert moved_keys > 0