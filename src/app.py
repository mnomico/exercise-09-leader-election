from contextlib import asynccontextmanager
from datetime import datetime, timezone
import threading
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Response
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.database import Base, engine, get_db
from src.election import (
    election_manager,
    handle_election_message,
    start_election,
)
from src.models import Node
from src.schemas import NodeCreate, NodeResponse, NodeUpdate

# Initialize DB tables gracefully so startup does not crash if DB is booting
try:
    Base.metadata.create_all(bind=engine)
except Exception:
    pass


class ElectionMessage(BaseModel):
    sender_id: Optional[int] = None


class CoordinatorMessage(BaseModel):
    leader_id: Optional[int] = None
    sender_id: Optional[int] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Try creating tables again in case DB became ready
    try:
        Base.metadata.create_all(bind=engine)
    except Exception:
        pass

    election_manager.start()
    yield
    election_manager.stop()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        db_status = "connected"
        count = db.query(Node).filter(Node.status == "active").count()
    except Exception:
        db_status = "disconnected"
        count = 0

    return {
        "status": "ok",
        "db": db_status,
        "nodes_count": count,
        "node_id": election_manager.node_id,
        "leader_id": election_manager.current_leader,
        "is_leader": election_manager.is_leader(),
        "leader": election_manager.current_leader,
        "state": election_manager.state,
    }


@app.get("/leader")
@app.get("/api/leader")
@app.get("/election/leader")
def get_leader():
    return {
        "leader_id": election_manager.current_leader,
        "node_id": election_manager.node_id,
        "is_leader": election_manager.is_leader(),
        "leader": election_manager.current_leader,
        "state": election_manager.state,
    }


@app.post("/election")
@app.post("/api/election")
def election_endpoint(msg: Optional[ElectionMessage] = None):
    if msg and msg.sender_id is not None:
        return handle_election_message(msg.sender_id)
    # Direct/manual trigger of election
    threading.Thread(target=start_election, daemon=True).start()
    return {"status": "ok", "message": "Election initiated"}


@app.post("/coordinator")
@app.post("/api/coordinator")
def coordinator_endpoint(msg: CoordinatorMessage):
    target_leader = msg.leader_id if msg.leader_id is not None else msg.sender_id
    if target_leader is None:
        raise HTTPException(status_code=400, detail="leader_id or sender_id is required")
    election_manager.handle_coordinator_message(target_leader)
    return {"status": "ok", "leader_id": election_manager.current_leader}


@app.post("/api/nodes", response_model=NodeResponse, status_code=201)
def register_node(node: NodeCreate, db: Session = Depends(get_db)):
    existing = db.query(Node).filter(Node.name == node.name).first()
    if existing:
        raise HTTPException(status_code=409, detail="Node already exists")
    db_node = Node(name=node.name, host=node.host, port=node.port)
    db.add(db_node)
    db.commit()
    db.refresh(db_node)
    return db_node


@app.get("/api/nodes", response_model=list[NodeResponse])
def list_nodes(db: Session = Depends(get_db)):
    return db.query(Node).all()


@app.get("/api/nodes/{name}", response_model=NodeResponse)
def get_node(name: str, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    return node


@app.put("/api/nodes/{name}", response_model=NodeResponse)
def update_node(name: str, update: NodeUpdate, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    if update.host is not None:
        node.host = update.host
    if update.port is not None:
        node.port = update.port
    node.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(node)
    return node


@app.delete("/api/nodes/{name}", status_code=204)
def delete_node(name: str, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    node.status = "inactive"
    node.updated_at = datetime.now(timezone.utc)
    db.commit()
    return Response(status_code=204)
