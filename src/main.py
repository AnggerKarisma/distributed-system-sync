# src/main.py

import os
import asyncio
import logging
from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from consensus.raft import RaftNode, NodeState
from nodes.lock_manager import LockManager
import uvicorn
from consensus.raft import RaftNode
from nodes.lock_manager import LockManager
from utils.consistent_hashing import ConsistentHashing
from nodes.queue_manager import QueueManager
from nodes.cache_manager import CacheManager, CacheState

# Konfigurasi
# Ambil dari environment variables (ditetapkan oleh docker-compose)
NODE_ID = os.environ.get('NODE_ID', 'http://localhost:8081')
PEERS_STR = os.environ.get('PEERS', 'http://localhost:8081,http://localhost:8082,http://localhost:8083')
PEERS = PEERS_STR.split(',')
API_PORT = int(os.environ.get('API_PORT', 8081))

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Main")

# Inisialisasi Objek
app = FastAPI()
lock_manager = LockManager()
raft_node = RaftNode(NODE_ID, PEERS, state_machine_applier=lock_manager.apply_command)
lock_manager.set_raft_node(raft_node) # Berikan referensi Raft ke LockManager
hash_ring = ConsistentHashing(PEERS)
queue_manager = QueueManager(NODE_ID, hash_ring)
cache_manager = CacheManager(NODE_ID, PEERS, cache_capacity=10)

# --- API Endpoints untuk Lock Manager ---

class LockRequest(BaseModel):
    lock_id: str
    client_id: str
    type: str # 'shared' or 'exclusive'

class ReleaseRequest(BaseModel):
    lock_id: str
    client_id: str

@app.post("/lock/acquire")
async def api_acquire_lock(request: LockRequest):
    if raft_node.state != NodeState.LEADER:
        return HTTPException(status_code=503, detail={"error": "Not a leader", "leader_id": raft_node.leader_id})
    result = await lock_manager.acquire_lock(request.lock_id, request.client_id, request.type)
    if not result['success']:
        if result['status'] == 'timeout':
            return HTTPException(status_code=408, detail=result) # 408 Request Timeout
        else:
            return HTTPException(status_code=409, detail=result) # 409 Conflict (atau 503)
    return result

@app.post("/lock/release")
async def api_release_lock(request: ReleaseRequest):
    if raft_node.state != NodeState.LEADER:
        return HTTPException(status_code=503, detail={"error": "Not a leader", "leader_id": raft_node.leader_id})

    result = await lock_manager.release_lock(request.lock_id, request.client_id)
    if not result['success']:
        return HTTPException(status_code=400, detail=result)
    return result

@app.get("/raft/status")
async def get_raft_status():
    """Endpoint untuk debugging."""
    return {
        "node_id": raft_node.node_id,
        "state": raft_node.state.name,
        "term": raft_node.current_term,
        "commit_index": raft_node.commit_index,
        "last_applied": raft_node.last_applied,
        "leader_id": raft_node.leader_id,
        "log_length": len(raft_node.log)
    }

@app.get("/lock/status")
async def get_lock_status():
    """Endpoint untuk debugging state kunci."""
    serializable_locks = {}
    for k, v in lock_manager.locks.items():
        serializable_locks[k] = {'type': v['type'], 'owners': list(v['owners'])}
        
    return {
        "node_id": raft_node.node_id,
        "is_leader": raft_node.state == NodeState.LEADER,
        "locks": serializable_locks
    }

# --- API Endpoints untuk Internal Raft ---

@app.post("/request_vote")
async def rpc_request_vote(request: Request):
    data = await request.json()
    response = await raft_node.handle_request_vote(data)
    return response

@app.post("/append_entries")
async def rpc_append_entries(request: Request):
    data = await request.json()
    response = await raft_node.handle_append_entries(data)
    return response

class QueuePushRequest(BaseModel):
    queue_name: str
    message: str

class QueuePopRequest(BaseModel):
    queue_name: str
    timeout: int = 5

@app.post("/queue/push")
async def api_queue_push(request: QueuePushRequest):
    """Endpoint publik untuk produser."""
    result = await queue_manager.push_message(request.queue_name, request.message)
    if not result['success']:
        return HTTPException(status_code=500, detail=result)
    return result

@app.post("/queue/pop")
async def api_queue_pop(request: QueuePopRequest):
    """Endpoint publik untuk consumer."""
    result = await queue_manager.pop_message(request.queue_name, request.timeout)
    if not result['success']:
        return HTTPException(status_code=500, detail=result)
    return result

# --- Endpoint Internal (Hanya untuk komunikasi antar-node) ---

class InternalPushRequest(BaseModel):
    queue_name: str
    message: str

@app.post("/queue/internal_push")
async def api_internal_push(request: InternalPushRequest):
    """Endpoint internal yang dipanggil oleh node lain."""
    logger.info(f"Received internal PUSH for '{request.queue_name}'")
    return await queue_manager._internal_push(request.queue_name, request.message)

class InternalPopRequest(BaseModel):
    queue_name: str
    timeout: int

@app.post("/queue/internal_pop")
async def api_internal_pop(request: InternalPopRequest):
    """Endpoint internal yang dipanggil oleh node lain."""
    logger.info(f"Received internal POP for '{request.queue_name}'")
    return await queue_manager._internal_pop(request.queue_name, request.timeout)

# --- API Endpoints untuk Cache Manager (Baru) ---

class CacheWriteRequest(BaseModel):
    key: str
    value: str

class CacheReadRequest(BaseModel):
    key: str
    
class InternalKeyRequest(BaseModel):
    key: str

@app.post("/cache/write_to_memory")
async def api_write_to_memory(request: CacheWriteRequest):
    """DEBUG: Tulis data langsung ke Main Memory (Redis) untuk tes."""
    await cache_manager._write_to_main_memory(request.key, request.value)
    return {"success": True, "message": f"'{request.key}' ditulis ke Main Memory (Redis)."}

@app.post("/cache/read")
async def api_cache_read(request: CacheReadRequest):
    """API Publik: Klien ingin membaca data (simulasi PrRd)."""
    data = await cache_manager.read_data(request.key)
    if data is None:
        return HTTPException(status_code=404, detail="Key not found")
    return {"key": request.key, "value": data}

@app.post("/cache/write")
async def api_cache_write(request: CacheWriteRequest):
    """API Publik: Klien ingin menulis data (simulasi PrWr)."""
    await cache_manager.write_data(request.key, request.value)
    return {"success": True, "message": f"'{request.key}' written."}

@app.get("/cache/status")
async def api_cache_status():
    """DEBUG: Lihat status cache LOKAL node ini."""
    return {
        "node_id": cache_manager.node_id,
        "cache_states": {k: v.value for k, v in cache_manager.cache_states.items()},
        "cache_data": cache_manager.cache_data
    }

# --- Endpoint Internal Cache (Hanya untuk komunikasi antar-node) ---

@app.post("/cache/handle_snoop_read")
async def api_handle_snoop_read(request: InternalKeyRequest):
    """Internal: Dipanggil oleh peer saat snoop read (BusRd)."""
    return await cache_manager.handle_snoop_read(request.key)

@app.post("/cache/handle_invalidate")
async def api_handle_invalidate(request: InternalKeyRequest):
    """Internal: Dipanggil oleh peer saat invalidate (BusRdX)."""
    return await cache_manager.handle_invalidate(request.key)

# --- Startup ---

@app.on_event("startup")
async def on_startup():
    # Jalankan main loop Raft di background
    asyncio.create_task(raft_node.run())
    logger.info(f"Node {NODE_ID} starting up...")

if __name__ == "__main__":
    logger.info(f"Starting server on 0.0.0.0:{API_PORT}")
    uvicorn.run(app, host="0.0.0.0", port=API_PORT)