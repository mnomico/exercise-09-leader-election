"""
Implementation of the Bully algorithm for leader election.

Functions implemented:
- start_election(): initiate an election, send ELECTION messages to higher-ID nodes
- handle_election_message(sender_id): respond to election from lower-ID node
- declare_victory(): announce self as leader to all nodes
- heartbeat_check(): periodically check if leader is alive
"""

import logging
import os
import re
import threading
import time
from typing import Dict, List, Optional
import requests

logger = logging.getLogger("election")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] [Bully Node %(node_id)s] %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


class NodeLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        kwargs.setdefault("extra", {})["node_id"] = self.extra.get("node_id", "?")
        return msg, kwargs


class BullyElection:
    def __init__(
        self,
        node_id: Optional[int] = None,
        peers: Optional[List[str]] = None,
        heartbeat_interval: Optional[float] = None,
        election_timeout: Optional[float] = None,
        coordinator_timeout: Optional[float] = None,
    ):
        self.node_id = node_id if node_id is not None else int(os.environ.get("NODE_ID", "1"))
        self.logger = NodeLoggerAdapter(logger, {"node_id": self.node_id})

        raw_peers = peers if peers is not None else os.environ.get("PEERS", "").split(",")
        self.peer_urls = [p.strip().rstrip("/") for p in raw_peers if p.strip()]

        self.heartbeat_interval = heartbeat_interval if heartbeat_interval is not None else float(
            os.environ.get("HEARTBEAT_INTERVAL", "2.0")
        )
        self.election_timeout = election_timeout if election_timeout is not None else float(
            os.environ.get("ELECTION_TIMEOUT", "1.5")
        )
        self.coordinator_timeout = coordinator_timeout if coordinator_timeout is not None else float(
            os.environ.get("COORDINATOR_TIMEOUT", "3.0")
        )

        self.current_leader: Optional[int] = None
        self.state: str = "follower"  # "follower", "candidate", "leader"
        self.election_in_progress: bool = False
        self.failed_heartbeats: int = 0
        self.running: bool = False

        self.election_lock = threading.Lock()
        self.coordinator_event = threading.Event()
        self.peer_map: Dict[int, str] = {}  # peer_id -> peer_url
        self.heartbeat_thread: Optional[threading.Thread] = None

        self._init_peer_map()

    def _extract_id_from_url(self, url: str) -> Optional[int]:
        # Matches patterns like node-2, node_2, node2
        m = re.search(r"node[-_]?(\d+)", url, re.IGNORECASE)
        if m:
            return int(m.group(1))
        # Matches patterns like host port 8082 -> 2
        m = re.search(r":808(\d)", url)
        if m:
            return int(m.group(1))
        return None

    def _init_peer_map(self):
        for url in self.peer_urls:
            pid = self._extract_id_from_url(url)
            if pid is not None and pid != self.node_id:
                self.peer_map[pid] = url

    def register_peer(self, peer_id: int, peer_url: Optional[str] = None):
        if peer_id == self.node_id:
            return
        if peer_url:
            self.peer_map[peer_id] = peer_url.rstrip("/")

    def resolve_peers(self):
        for url in self.peer_urls:
            pid = self._extract_id_from_url(url)
            if pid is None:
                try:
                    resp = requests.get(f"{url}/leader", timeout=0.8)
                    if resp.status_code == 200:
                        data = resp.json()
                        remote_id = data.get("node_id")
                        if remote_id and remote_id != self.node_id:
                            self.peer_map[remote_id] = url
                except Exception:
                    pass

    def get_peers(self) -> Dict[int, str]:
        return dict(self.peer_map)

    def is_leader(self) -> bool:
        return self.state == "leader" or self.current_leader == self.node_id

    def start_election(self):
        with self.election_lock:
            if self.election_in_progress:
                self.logger.info("Election already in progress; skipping duplicate start.")
                return
            self.election_in_progress = True
            self.state = "candidate"
            self.current_leader = None
            self.coordinator_event.clear()

        self.logger.info("Initiating Bully election.")

        higher_peers = [(pid, url) for pid, url in self.get_peers().items() if pid > self.node_id]

        if not higher_peers:
            self.logger.info("No peers with higher ID found. Self-proclaiming victory.")
            self.declare_victory()
            return

        self.logger.info(f"Sending ELECTION message to higher-ID peers: {[pid for pid, _ in higher_peers]}")

        received_ok = False

        def send_election(pid: int, url: str):
            nonlocal received_ok
            try:
                resp = requests.post(
                    f"{url}/election",
                    json={"sender_id": self.node_id},
                    timeout=self.election_timeout,
                )
                if resp.status_code == 200:
                    self.logger.info(f"Received OK response from Node {pid}")
                    received_ok = True
            except requests.RequestException:
                self.logger.debug(f"Node {pid} at {url} did not respond to ELECTION within timeout")

        threads = []
        for pid, url in higher_peers:
            t = threading.Thread(target=send_election, args=(pid, url), daemon=True)
            t.start()
            threads.append(t)

        for t in threads:
            t.join(timeout=self.election_timeout + 0.5)

        if not received_ok:
            self.logger.info("No higher-ID peers answered OK. Declaring victory as leader.")
            self.declare_victory()
        else:
            self.logger.info(
                f"Received OK from higher node. Waiting up to {self.coordinator_timeout}s for COORDINATOR message..."
            )
            signaled = self.coordinator_event.wait(timeout=self.coordinator_timeout)
            if not signaled and self.state != "leader":
                self.logger.warning("COORDINATOR message timed out. Higher node may have crashed. Restarting election.")
                with self.election_lock:
                    self.election_in_progress = False
                self.start_election()
            else:
                with self.election_lock:
                    self.election_in_progress = False

    def handle_election_message(self, sender_id: int):
        self.logger.info(f"Received ELECTION message from Node {sender_id}")
        if sender_id < self.node_id:
            if self.is_leader():
                # If this node is already the leader, re-announce victory so the lower node learns leader immediately
                threading.Thread(target=self.declare_victory, daemon=True).start()
            else:
                # Start own election in background so OK is returned immediately to caller
                threading.Thread(target=self.start_election, daemon=True).start()
            return {"status": "ok", "message": "OK"}
        return {"status": "ok", "message": "OK"}

    def declare_victory(self):
        with self.election_lock:
            self.current_leader = self.node_id
            self.state = "leader"
            self.election_in_progress = False
            self.failed_heartbeats = 0
            self.coordinator_event.set()

        self.logger.info("VICTORY! Broadcasting COORDINATOR message to all peers...")

        peers = self.get_peers()

        def send_coord(pid: int, url: str):
            try:
                requests.post(
                    f"{url}/coordinator",
                    json={"leader_id": self.node_id, "sender_id": self.node_id},
                    timeout=1.5,
                )
                self.logger.info(f"COORDINATOR delivered to Node {pid} at {url}")
            except requests.RequestException:
                self.logger.debug(f"Could not deliver COORDINATOR to Node {pid} at {url} (node may be down)")

        threads = []
        for pid, url in peers.items():
            t = threading.Thread(target=send_coord, args=(pid, url), daemon=True)
            t.start()
            threads.append(t)

        for t in threads:
            t.join(timeout=1.8)

    def handle_coordinator_message(self, leader_id: int):
        self.logger.info(f"Received COORDINATOR message: new leader is Node {leader_id}")
        with self.election_lock:
            self.current_leader = leader_id
            if leader_id == self.node_id:
                self.state = "leader"
            else:
                self.state = "follower"
            self.election_in_progress = False
            self.failed_heartbeats = 0
            self.coordinator_event.set()

    def heartbeat_check(self):
        self.logger.info("Heartbeat checker thread started.")
        while self.running:
            time.sleep(self.heartbeat_interval)
            if not self.running:
                break

            if self.state == "leader":
                continue

            if self.election_in_progress:
                continue

            if self.current_leader is None:
                self.logger.warning("No leader set while in follower state. Starting election.")
                threading.Thread(target=self.start_election, daemon=True).start()
                continue

            leader_url = self.get_peers().get(self.current_leader)
            if not leader_url:
                self.resolve_peers()
                leader_url = self.get_peers().get(self.current_leader)

            if not leader_url:
                self.logger.warning(f"URL for leader {self.current_leader} unknown. Starting election.")
                self.current_leader = None
                threading.Thread(target=self.start_election, daemon=True).start()
                continue

            alive = False
            try:
                resp = requests.get(f"{leader_url}/health", timeout=1.0)
                if resp.status_code == 200:
                    alive = True
            except requests.RequestException:
                alive = False

            if alive:
                self.failed_heartbeats = 0
            else:
                self.failed_heartbeats += 1
                self.logger.warning(
                    f"Heartbeat to leader {self.current_leader} failed ({self.failed_heartbeats}/2)"
                )
                if self.failed_heartbeats >= 2:
                    self.logger.error(
                        f"Leader {self.current_leader} unreachable after 2 failed heartbeats. Starting election!"
                    )
                    self.current_leader = None
                    self.failed_heartbeats = 0
                    threading.Thread(target=self.start_election, daemon=True).start()

    def start(self):
        self.running = True
        self.heartbeat_thread = threading.Thread(target=self.heartbeat_check, daemon=True)
        self.heartbeat_thread.start()

        def startup_discovery_and_election():
            time.sleep(0.5)
            self.resolve_peers()
            self.start_election()

        threading.Thread(target=startup_discovery_and_election, daemon=True).start()

    def stop(self):
        self.running = False
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join(timeout=1.0)


# Global singleton instance for module-level functions
election_manager = BullyElection()


def start_election():
    return election_manager.start_election()


def handle_election_message(sender_id: int):
    return election_manager.handle_election_message(sender_id)


def declare_victory():
    return election_manager.declare_victory()


def heartbeat_check():
    return election_manager.heartbeat_check()
