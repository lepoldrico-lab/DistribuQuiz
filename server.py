"""
DistribuQuiz - Server
======================
Distributed Quiz-System: Multiple Servers working together.
One of them is Quiz Master (Leader), the others are Backups.
When the Quiz Master fails, a Backup takes over automatically.

This is a hybrid architecture: client-server between players and the Quiz Master,
peer-to-peer among the servers themselves (every server talks directly to every
other server, no fixed hierarchy besides the elected leader). Concurrency is
handled with multithreading throughout — see the fixed background threads started
in run(), plus one extra short-lived thread per accepted connection.

Architecture overview:
- Discovery:   UDP broadcast on DISCOVERY_PORT — lightweight, connectionless.
                Servers announce themselves to the whole network without knowing addresses in advance.
                This discovery mechanism is one-way (new server -> existing ones); see discovery_loop()
                for when it re-runs, and find_quiz_master() in client.py for how clients discover servers.
- Game/Sync:   TCP on individual ports (BASE_PORT+) — reliable, ordered delivery for game events
                and state replication to backups.
- Client push: The server connects BACK to the client on the client's own listen port (CLIENT_BASE_PORT+).
                This "reverse TCP" avoids the client needing a known address.
- Election:    Simplified Bully Algorithm — every server knows all peers; the one with the
                highest UUID wins and notifies the others. No rounds of messaging needed.
- Replication: The Quiz Master pushes its full game_state to all backups after every event,
                so any backup can instantly take over with zero data loss. This is PASSIVE
                REPLICATION / the PRIMARY-BACKUP PROTOCOL (only the primary/leader executes
                and computes; backups just overwrite their state) — NOT active replication
                (where every replica independently executes the same operations).

Exam term quick-reference (search this file for the CAPS keyword to jump to the code):
- UNICAST vs BROADCAST vs MULTICAST: send_msg() is unicast (one-to-one, TCP); UDP discovery
                uses BROADCAST (one-to-all on the subnet, 255.255.255.255); _multicast_to_players()
                is a multicast in the application sense (one-to-many) but is really a loop of
                unicasts — no IP multicast group address is used.
- SYNCHRONOUS vs ASYNCHRONOUS communication: "who_is_leader" (handle_message) is a synchronous
                / blocking request-reply — sender waits on recv() for the answer. Nearly every
                other message (heartbeat, state_sync, join_game, ...) is asynchronous / fire-
                and-forget — send_msg() does not wait for or read any application-level reply.
- PUSH vs PULL: state_sync and heartbeats are PUSH-based (leader/peer proactively sends,
                receiver does nothing to request it). find_quiz_master() in client.py is PULL-
                based (client actively asks / scans for the leader).
- MARSHALLING / SERIALIZATION: json.dumps() turns a Python dict into a byte stream for the
                wire; json.loads() on the receiving side unmarshals / deserializes it back.
- FAULT MODEL: this system assumes CRASH-STOP / FAIL-SILENT faults only (a server works
                correctly or stops completely). It does NOT tolerate BYZANTINE FAULTS
                (arbitrary, malicious, or corrupted behavior) — a compromised or buggy server
                that lies (e.g. claims a false UUID or sends bad state_sync data) is not handled.
- CONSISTENCY / CAP THEOREM: this design favors Availability over Consistency (an AP system).
                There is no QUORUM or CONSENSUS protocol (e.g. Paxos/Raft) guarding the election
                — during a NETWORK PARTITION each partition can independently elect its own
                "highest UUID" leader, i.e. a SPLIT-BRAIN (two Quiz Masters at once) is possible.
- SINGLE POINT OF FAILURE (SPOF): the Quiz Master is a SPOF for the game logic; the Backups +
                election mechanism exist specifically to remove that SPOF (→ HIGH AVAILABILITY).
- FULL MESH topology: every server maintains a connection-capable link to every other server
                (via peers{}) — contrast with the STAR topology of the client-server side, where
                all clients connect to one central Quiz Master.
- MUTUAL EXCLUSION / CRITICAL SECTION: self.lock (threading.Lock) protects game_state and peers
                from concurrent read/write by multiple threads — a classic local mutual-exclusion
                mechanism, not a distributed one (no distributed mutual exclusion / no Ricart-
                Agrawala etc. is used since only one process, the leader, ever writes canonical state).
- PHYSICAL/WALL-CLOCK TIME vs LOGICAL CLOCKS: timestamps here (time.time(), last_seen,
                question_start_time) are physical/wall-clock time. No LOGICAL CLOCKS (Lamport
                timestamps) or VECTOR CLOCKS are used — ordering relies on TCP's own in-order,
                reliable delivery per connection, not on causal/logical ordering across servers.

Start:  python3 server.py
"""

import socket
import threading
import json
import time
import random
import os
import uuid
import sys

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

DISCOVERY_PORT  = 55000            # Broadcast port for server discovery
BASE_PORT       = 5100             # Servers run on ports 5100, 5101, ... — kept below 49152 (Windows'
                                    # dynamic/ephemeral port range) so Hyper-V/WSL's NAT port exclusions
                                    # can never land on it; ports above 49152 are excluded on a rotating
                                    # basis and can make _find_free_port() fail with no code change at all.
HEARTBEAT_SEC   = 2                # Every 2 sec: "I'm alive!"
TIMEOUT_SEC     = 15               # After 15 sec without signal: server failed. Set well above
                                    # HEARTBEAT_SEC so a single lost/omitted heartbeat (an omission
                                    # fault) doesn't falsely trigger an election.
QUESTION_SEC    = 10               # How long players have per question
PAUSE_SEC       = 3                # Pause between questions (showing solution)
MIN_PLAYERS     = 2                # Minimum number of players to start
QUESTIONS_FILE  = "questions.json" # File with the questions (JSON)


# ─────────────────────────────────────────────
# HELPER: Send a Message
# ─────────────────────────────────────────────

def send_msg(host, port, data: dict):
    """
    Sends a JSON message to another server or client.
    Input: host (str), port (int), data (dict)
    Calculation: Opens a TCP connection, sends the JSON-encoded data, and closes the connection.
    This is UNICAST (one sender, one receiver) and ASYNCHRONOUS / FIRE-AND-FORGET — it never
    reads a reply, so the caller only learns "did the send succeed", not "did the receiver
    do anything with it". json.dumps() here is the MARSHALLING / SERIALIZATION step.
    Delivery guarantee: at-most-once (TCP guarantees no duplication/corruption in transit, but
    there is no retry here, so on failure the message is simply dropped — see the except below).
    Output: bool (True if sent successfully, False otherwise)
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(3)
            s.connect((host, port))
            s.sendall(json.dumps(data).encode())
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────
# MAIN CLASS: Server
# ─────────────────────────────────────────────

class QuizServer:

    # ─────────────────────────────────────────
    # 1. INITIALIZATION
    # ─────────────────────────────────────────

    def __init__(self):
        """
        Initializes the Quiz Server.
        Input: None
        Calculation:
        Generates a unique server ID, finds a free port, initializes leader info, peer list, and game state.
        Output: None
        """
        self.server_id = str(uuid.uuid4())  # random UUID — this server's unique ID / node identifier; the highest one wins the Bully election
        self.port      = self._find_free_port()

        self.leader_id   = None
        self.leader_port = None

        self.peers = {}         # peer list / group view / membership table: {server_id: {port, host, last_seen}}
        self.lock  = threading.Lock()  # MUTUAL EXCLUSION lock: guards the CRITICAL SECTIONS that read/write
                                       # peers and game_state — both are shared state touched by multiple
                                       # threads (heartbeat, quiz loop, message handler) and would otherwise
                                       # be subject to RACE CONDITIONS. This is local/in-process mutual
                                       # exclusion only, not distributed mutual exclusion.

        self._last_sync_phase        = None  # Used to suppress redundant [Sync] prints
        self._last_sync_player_count = -1

        # Quiz game state — Backups keep a copy of this
        self.game_state = {
            "phase": "lobby",           # lobby | question | results | finished
            "players": {},              # {player_uid: {name, port, host, score}}
            "current_question_idx": -1, # Index of the current question
            "current_question": None,   # The current question being asked
            "answers_this_round": {},   # {player_uid: answer}
            "question_start_time": 0,   # Timestamp when the current question was sent
            "questions_order": []       # Shuffled order — synced to backups
        }

        self.questions = self._load_questions()

        print(f"\n{'='*55}")
        print(f"  DistribuQuiz Server is running!")
        print(f"  Server-ID : {self.server_id}")
        print(f"  Port      : {self.port}")
        print(f"  Questions : {len(self.questions)} loaded")
        print(f"{'='*55}\n")

    def _find_free_port(self):
        """
        Searches for a free port starting at BASE_PORT.
        Input: None
        Calculation: Scans ports from BASE_PORT to BASE_PORT+20 and returns the first free one.
        Output: int (free port number)
        """
        for p in range(BASE_PORT, BASE_PORT + 20):
            try:
                test = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                test.bind(("", p))
                test.close()
                return p
            except OSError:
                continue
        raise RuntimeError("No free port was found!")

    def _load_questions(self):
        """
        Loads questions from the JSON file.
        Input: None
        Calculation: Reads the questions file and returns the list.
        Output: list
        """
        if not os.path.exists(QUESTIONS_FILE):
            print(f"Error: {QUESTIONS_FILE} not found!")
            return []
        with open(QUESTIONS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)

    # ─────────────────────────────────────────
    # 2. STARTUP — entry point, starts all threads
    # ─────────────────────────────────────────

    def run(self):
        """
        Starts the Quiz Server.
        Input: None
        Calculation:
        Starts all background threads, broadcasts presence to find other servers,
        and triggers an election if no Quiz Master is found.
        Output: None
        """
        threads = [
            threading.Thread(target=self.listen_for_connections, daemon=True), # Accept TCP connections
            threading.Thread(target=self.send_heartbeats,        daemon=True), # Send "I'm alive" signals
            threading.Thread(target=self.check_for_failures,     daemon=True), # Detect crashed servers
            threading.Thread(target=self.run_quiz,               daemon=True), # Run quiz (only if leader)
            threading.Thread(target=self.discovery_listener,     daemon=True), # Answer UDP discovery pings
            threading.Thread(target=self.discovery_loop,         daemon=True), # Periodically re-discover
        ]
        for t in threads:
            t.start()

        time.sleep(0.5)
        self.broadcast_presence()   # Find other servers in the network

        # Wait for heartbeats to propagate and for any existing Quiz Master
        # to send a new_leader notification.  Only start an election if still unknown.
        time.sleep(6)
        if self.leader_id is None:
            print("[Voting] No Quiz Master found → starting election...")
            self.start_election()

        print("\n[Server] Running. Press Ctrl+C to stop.\n")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[Server] Stopping...")

    # ─────────────────────────────────────────
    # 3. DISCOVERY — finding other servers
    # ─────────────────────────────────────────

    def broadcast_presence(self, quiet=False, retries=3, retry_delay=1.0):
        """
        Broadcasts presence via UDP to find other servers in the network.
        Input: quiet (bool) — when True, only logs newly discovered servers
               retries (int) — how many broadcast attempts to make if no servers found
               retry_delay (float) — seconds to wait between retries
        Calculation:
        Sends a UDP broadcast to DISCOVERY_PORT and collects responses from other servers.
        Retries multiple times so a new server reliably finds an existing cluster.
        This is BROADCAST communication (one-to-all on the local subnet via 255.255.255.255)
        over UDP (CONNECTIONLESS, unreliable — no delivery guarantee, no ordering, which is why
        retries are needed here at the application level to approximate at-least-once delivery).
        This is also SERVICE DISCOVERY: a new node finds existing group members without knowing
        their addresses in advance (PULL-based from this server's point of view — it actively asks).
        Output: None
        """
        if not quiet:
            print("[Discovery] Searching for servers in the network...")

        found = 0
        for attempt in range(retries):
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.settimeout(2)

            sock.sendto(
                json.dumps({"type": "discover_server"}).encode(),
                ("255.255.255.255", DISCOVERY_PORT)
            )

            while True:
                try:
                    data, addr = sock.recvfrom(4096)
                    msg = json.loads(data.decode())

                    sid = msg["server_id"]
                    if sid == self.server_id:
                        continue

                    with self.lock:
                        is_new = sid not in self.peers
                        self.peers[sid] = {
                            "port": msg["port"],
                            "host": addr[0],
                            "last_seen": time.time()
                        }
                    found += 1
                    if not quiet or is_new:
                        print(f"[Discovery] Server found: ID={sid} IP={addr[0]} Port={msg['port']}")

                except socket.timeout:
                    break

            sock.close()

            if found > 0:
                break  # found servers — no need to retry

            if attempt < retries - 1:
                if not quiet:
                    print(f"[Discovery] No response yet, retrying ({attempt + 2}/{retries})...")
                time.sleep(retry_delay)

        if not quiet:
            print(f"[Discovery] {found} server(s) found")

    def discovery_listener(self):
        """
        Listens for UDP discovery broadcasts and responds with own server info.
        Input: None
        Calculation:
        Creates a UDP socket on DISCOVERY_PORT and answers "discover_server" messages.
        This is the passive/server side of SERVICE DISCOVERY: it PUSHes a direct UNICAST reply
        (sock.sendto(..., addr)) back to the asking node, even though the request itself arrived
        via BROADCAST — a simple REQUEST-REPLY pattern layered on top of connectionless UDP.
        Output: None
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind(("0.0.0.0", DISCOVERY_PORT))
        while True:
            try:
                data, addr = sock.recvfrom(4096)
                msg = json.loads(data.decode())
                if msg.get("type") == "discover_server":
                    sock.sendto(json.dumps({
                        "type": "server_present",
                        "server_id": self.server_id,
                        "port": self.port
                    }).encode(), addr)
            except Exception:
                pass

    def discovery_loop(self):
        """
        Periodically re-broadcasts presence to discover newly started servers.
        Input: None
        Calculation: Every 60 seconds, quietly checks for new servers.
        This is POLLING (periodic active check) as opposed to an event-driven / PUSH mechanism —
        a newly started server is only found once this loop's interval elapses, not instantly.
        Output: None
        """
        while True:
            time.sleep(60)
            self.broadcast_presence(quiet=True)

    # ─────────────────────────────────────────
    # 4. ELECTION / VOTING — choosing the Quiz Master
    # ─────────────────────────────────────────

    def start_election(self):
        """
        Elects the Quiz Master using a simplified BULLY ALGORITHM (COORDINATOR ELECTION problem).
        Input: None
        Calculation:
        Each server already knows all peers (via discovery + heartbeats), so no
        election messages are exchanged.  Every participant simply picks the server
        with the highest UUID as winner and announces the result via new_leader.
        The server with the highest ID "bullies" the others into accepting it.
        If this server wins AND it was not the leader before, it checks whether a
        game is running and calls _continue_game to take over seamlessly (FAILOVER).
        Classic Bully Algorithm sends ELECTION / OK(ANSWER) / COORDINATOR messages between
        candidates; this variant skips straight to the COORDINATOR announcement (new_leader)
        because membership is already known, trading message rounds for a small SPLIT-BRAIN
        risk if the peer view is stale/partitioned (see module docstring, CAP theorem note).
        Output: None
        """
        print(f"\n[Voting] Election started! My ID: {self.server_id}")

        with self.lock:
            all_ids = {self.server_id: self.port}
            for sid, info in self.peers.items():
                all_ids[sid] = info["port"]

        winner_id   = max(all_ids.keys())
        winner_port = all_ids[winner_id]

        was_leader_before = (self.leader_id == self.server_id)
        self.leader_id    = winner_id
        self.leader_port  = winner_port

        if winner_id == self.server_id:
            print(f"[Voting] I am the new Quiz Master! (ID: {self.server_id})")
            if not was_leader_before:
                if self.game_state["phase"] in ["question", "results"]:
                    print(f"[Voting] Taking over the running game from the failed server!")
                    threading.Thread(target=self._continue_game, daemon=True).start()
        else:
            print(f"[Voting] Quiz Master chosen: Server {winner_id} (Port {winner_port})")

        with self.lock:
            peers_copy = dict(self.peers)

        for sid, info in peers_copy.items():
            send_msg(info["host"], info["port"], {
                "type": "new_leader",
                "leader_id": winner_id,
                "leader_port": winner_port
            })

    # ─────────────────────────────────────────
    # 5. HEARTBEAT & FAULT TOLERANCE
    # ─────────────────────────────────────────

    def send_heartbeats(self):
        """
        Sends "I'm alive" heartbeat messages to all peers every HEARTBEAT_SEC seconds.
        Input: None
        Calculation: Every HEARTBEAT_SEC seconds, sends a heartbeat to all known peers.
        This is a PUSH-based / active HEARTBEAT PROTOCOL: every server proactively tells every
        other server it's alive (ALL-TO-ALL, FULL MESH), as opposed to a PULL-based failure
        detector where peers would have to ping/poll each other to check liveness.
        Output: None
        """
        while True:
            time.sleep(HEARTBEAT_SEC)
            with self.lock:
                peers_copy = dict(self.peers)
            for _, info in peers_copy.items():
                send_msg(info["host"], info["port"], {
                    "type": "heartbeat",
                    "server_id": self.server_id,
                    "port": self.port
                })

    def check_for_failures(self):
        """
        This is the FAILURE DETECTOR side of the heartbeat mechanism (TIMEOUT-BASED / crash-stop
        failure detection): checks regularly whether any server has stopped sending heartbeats.
        Input: None
        Calculation:
        Every HEARTBEAT_SEC seconds, checks last_seen for each peer.
        TIMEOUT_SEC (15 s) is 7x HEARTBEAT_SEC (2 s), so a server must miss
        several consecutive heartbeats before being declared failed — this
        tolerates brief network hiccups without triggering a false election.
        If the failed server was the Quiz Master, a new election starts.
        Output: None
        """
        while True:
            time.sleep(HEARTBEAT_SEC)
            now    = time.time()
            failed = []

            with self.lock:
                for sid, info in self.peers.items():
                    if now - info["last_seen"] > TIMEOUT_SEC:
                        failed.append(sid)

            for sid in failed:
                with self.lock:
                    self.peers.pop(sid, None)
                print(f"\n[Fault Tolerance] Server {sid} failed!")

                if sid == self.leader_id:
                    print(f"[Fault Tolerance] Quiz Master failed! Starting new election...")
                    self.leader_id = None
                    threading.Thread(target=self.start_election, daemon=True).start()

    # ─────────────────────────────────────────
    # 6. INCOMING CONNECTIONS — receiving messages
    # ─────────────────────────────────────────

    def listen_for_connections(self):
        """
        Listens for incoming TCP connections on the server's port.
        Input: None
        Calculation:
        Creates a TCP socket, binds to the server port, and for each connection
        starts a new thread to handle the message.
        THREAD-PER-CONNECTION model over a CONNECTION-ORIENTED (TCP) socket — contrast with the
        CONNECTIONLESS UDP sockets used for discovery, which need no accept()/listen() at all.
        Output: None
        """
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind(("0.0.0.0", self.port))
        server_sock.listen(20)
        while True:
            conn, addr = server_sock.accept()
            # A short-lived thread per connection, separate from the fixed background threads above
            threading.Thread(target=self.handle_message, args=(conn, addr), daemon=True).start()

    def handle_message(self, conn, addr):
        """
        Handles a single incoming connection.
        Input: conn (socket), addr (tuple)
        Calculation:
        Receives data, decodes JSON, and either answers "who_is_leader" directly
        or forwards to _process for full handling.
        Output: None
        """
        try:
            conn.settimeout(3)
            data = conn.recv(16384)
            if not data:
                return
            msg = json.loads(data.decode())

            if msg.get("type") == "who_is_leader":
                # This is the reply a client's port-scan (its discovery / lookup step) is waiting for
                # Handled here (not in _process) because it needs a SYNCHRONOUS / blocking REQUEST-REPLY
                # on the same connection — all other message types are ASYNCHRONOUS / fire-and-forget.
                if not self.leader_id or not self.leader_port:
                    conn.sendall(json.dumps({"leader_id": None, "leader_port": None}).encode())
                    return
                # The leader is not in its own peers dict, so handle that case separately
                if self.leader_id == self.server_id:
                    leader_host = self._get_own_host()
                else:
                    leader_host = self.peers.get(self.leader_id, {}).get("host", "127.0.0.1")
                conn.sendall(json.dumps({
                    "leader_id":   self.leader_id,
                    "leader_port": self.leader_port,
                    "leader_host": leader_host
                }).encode())
                return

            self._process(msg, addr[0])
        except Exception:
            pass
        finally:
            conn.close()

    def _process(self, msg, peer_host="127.0.0.1"):
        """
        Routes an incoming message to the correct handler based on its type.
        Input: msg (dict), peer_host (str)
        Calculation:
        Dispatches server-to-server messages (hello, welcome, heartbeat, new_leader, state_sync)
        and client-to-server messages (join_game, submit_answer).
        No RPC framework / MIDDLEWARE (e.g. gRPC, Java RMI) is used — this is a hand-rolled
        MESSAGE-ORIENTED protocol: plain JSON dicts over raw TCP sockets, manually dispatched
        by a "type" string field, i.e. a poor-man's remote method dispatch table.
        Output: None
        """
        t = msg.get("type")

        # ── Server ↔ Server ──────────────────────────────────────────

        if t == "hello":
            # Reserved for a future direct handshake/greeting; never actually sent or triggered —
            # servers currently find each other via UDP broadcast_presence() instead, so this
            # code path is effectively unused / a no-op today.
            # New server discovered: add to peers and send back a welcome
            with self.lock:
                self.peers[msg["server_id"]] = {
                    "port": msg["port"],
                    "host": peer_host,
                    "last_seen": time.time()
                }
            print(f"[Discovery] New server: ID={msg['server_id']} Port={msg['port']}")
            send_msg(peer_host, msg["port"], {
                "type": "welcome",
                "server_id": self.server_id,
                "port": self.port,
                "leader_id": self.leader_id,
                "leader_port": self.leader_port
            })

        elif t == "welcome":
            # Would also re-trigger / restart an election when a peer's handshake reply comes in,
            # but since "hello" above is never sent, this branch never actually runs or fires —
            # the UDP-based discovery flow doesn't route through here.
            # Another server replied to our hello: add to peers, update leader if known
            with self.lock:
                self.peers[msg["server_id"]] = {
                    "port": msg["port"],
                    "host": peer_host,
                    "last_seen": time.time()
                }
            if msg.get("leader_id"):
                self.leader_id   = msg["leader_id"]
                self.leader_port = msg["leader_port"]
                print(f"[Discovery] Known Quiz Master: Server {self.leader_id}")
            threading.Thread(target=self.start_election, daemon=True).start()

        elif t == "heartbeat":
            # Update last_seen timestamp for the sending server.
            # The else-branch is how new servers join the peer list without a
            # separate handshake: once server A heartbeats to B, B adds A automatically.
            with self.lock:
                if msg["server_id"] in self.peers:
                    self.peers[msg["server_id"]]["last_seen"] = time.time()
                else:
                    self.peers[msg["server_id"]] = {
                        "port": msg["port"],
                        "host": peer_host,
                        "last_seen": time.time()
                    }

        elif t == "new_leader":
            # Accept and store the new leader information
            self.leader_id   = msg["leader_id"]
            self.leader_port = msg["leader_port"]
            if self.leader_id == self.server_id:
                print(f"[Voting] I am the new Quiz Master!")
            else:
                print(f"[Voting] New Quiz Master: Server {self.leader_id}")

        elif t == "state_sync":
            # Backup: replace local game state with the Quiz Master's state.
            # This is PASSIVE REPLICATION / the PRIMARY-BACKUP PROTOCOL in action — the backup
            # does not re-execute any logic, it just overwrites its state with a fresh STATE
            # TRANSFER from the primary (as opposed to OPERATION/LOG TRANSFER + re-execution,
            # which is what ACTIVE REPLICATION / STATE MACHINE REPLICATION would do).
            with self.lock:
                self.game_state = msg["game_state"]
            phase       = self.game_state.get("phase", "?")
            num_players = len(self.game_state.get("players", {}))
            if phase != self._last_sync_phase or num_players != self._last_sync_player_count:
                print(f"[Sync] Game state updated (Phase: {phase}, Players: {num_players})")
                self._last_sync_phase        = phase
                self._last_sync_player_count = num_players

        # ── Client → Server ──────────────────────────────────────────

        elif t == "join_game":
            # Redirect to Quiz Master if this server is a backup.
            # LEADER FORWARDING / REDIRECTION: a request that lands on a non-primary replica is
            # bounced back to the client with the primary's address, rather than the backup
            # silently handling (or dropping) it — typical of primary-backup systems.
            if not self._is_quiz_master():
                send_msg("127.0.0.1", msg["client_port"], {
                    "type": "redirect",
                    "leader_port": self.leader_port
                })
                return

            player_name = msg["player_name"]
            client_port = msg["client_port"]
            client_host = msg.get("client_host", "127.0.0.1")
            client_uid  = msg.get("player_uid")  # None on a player's very first join

            reconnect_data = None
            num_players    = 0
            assigned_uid   = None

            with self.lock:
                if self.game_state["phase"] == "finished":
                    self.game_state = {
                        "phase": "lobby",
                        "players": {},
                        "current_question_idx": -1,
                        "current_question": None,
                        "answers_this_round": {},
                        "question_start_time": 0,
                        "questions_order": []
                    }
                    print(f"[Quiz] New game started (player connected)")

                # Prefer the UUID the client already holds. Fall back to matching by name
                # when it doesn't resolve — e.g. the client process was restarted (same
                # terminal, same player) and lost its in-memory UUID — so the same human
                # continues as the player they already were instead of being listed twice.
                existing_uid = client_uid if client_uid in self.game_state["players"] else None
                if existing_uid is None:
                    for uid, p in self.game_state["players"].items():
                        if p["name"] == player_name:
                            existing_uid = uid
                            break

                if existing_uid is not None:
                    # Reconnect: update address, keep score
                    self.game_state["players"][existing_uid]["port"] = client_port
                    self.game_state["players"][existing_uid]["host"] = client_host
                    score  = self.game_state["players"][existing_uid]["score"]
                    q_data = None
                    if self.game_state["phase"] == "question" and self.game_state["current_question"]:
                        elapsed   = time.time() - self.game_state["question_start_time"]
                        remaining = max(1, int(QUESTION_SEC - elapsed))
                        q_data = {
                            "type": "new_question",
                            "question_number": self.game_state["current_question_idx"] + 1,
                            "total_questions": len(self.game_state.get("questions_order") or self.questions),
                            "question": self.game_state["current_question"]["question"],
                            "time_limit": remaining
                        }
                    reconnect_data = {"uid": existing_uid, "score": score, "q_data": q_data}
                else:
                    assigned_uid = str(uuid.uuid4())
                    self.game_state["players"][assigned_uid] = {
                        "name": player_name,
                        "uid": assigned_uid,
                        "port": client_port,
                        "host": client_host,
                        "score": 0
                    }
                    num_players = len(self.game_state["players"])

            if reconnect_data is not None:
                payload = {
                    "type": "reconnected",
                    "player_name": player_name,
                    "player_uid": reconnect_data["uid"],
                    "score": reconnect_data["score"],
                    "message": f"Welcome back, {player_name}! Your score: {reconnect_data['score']} point(s)."
                }
                if reconnect_data["q_data"]:
                    payload["current_question"] = reconnect_data["q_data"]
                send_msg(client_host, client_port, payload)
                self._sync_to_backups()
                print(f"[Quiz] Player '{player_name}' reconnected (Score: {reconnect_data['score']})")
                return

            print(f"[Quiz] New player: '{player_name}' (total: {num_players})")
            send_msg(client_host, client_port, {
                "type": "joined",
                "player_name": player_name,
                "player_uid": assigned_uid,
                "message": f"Welcome {player_name}!"
            })
            self._multicast_to_players({
                "type": "player_joined",
                "player_name": player_name,
                "total_players": num_players
            })
            self._sync_to_backups()

        elif t == "submit_answer":
            if not self._is_quiz_master():
                return

            player_uid = msg.get("player_uid")
            answer     = msg["answer"]

            player_name = None
            with self.lock:
                if self.game_state["phase"] != "question":
                    return
                if player_uid not in self.game_state["players"]:
                    return
                self.game_state["answers_this_round"][player_uid] = answer
                player_name = self.game_state["players"][player_uid]["name"]

            print(f"[Quiz] {player_name} answered: {'TRUE' if answer else 'FALSE'}")

    # ─────────────────────────────────────────
    # 7. QUIZ LOGIC — only the Quiz Master runs this
    # ─────────────────────────────────────────

    def run_quiz(self):
        """
        Waits for enough players in the lobby, then starts the game.
        Input: None
        Calculation:
        Loops every 2 seconds; if this server is the Quiz Master and the lobby has
        enough players, triggers _start_game.
        Output: None
        """
        while True:
            time.sleep(2)
            if not self._is_quiz_master():
                continue
            if self.game_state["phase"] != "lobby":
                continue

            with self.lock:
                num_players = len(self.game_state["players"])

            if num_players >= MIN_PLAYERS:
                print(f"\n[Quiz] {num_players} players ready. Starting game!")
                self._start_game()

    def _start_game(self):
        """
        Shuffles the questions and starts playing from question 0.
        Input: None
        Calculation: Shuffles the question list, stores it in game_state, and calls _play_questions.
        Output: None
        """
        shuffled = list(self.questions)
        random.shuffle(shuffled)
        with self.lock:
            self.game_state["questions_order"] = shuffled
        self._sync_to_backups()
        self._play_questions(start_idx=0)

    def _play_questions(self, start_idx):
        """
        Plays all questions starting from start_idx.
        Input: start_idx (int)
        Calculation:
        For each question: updates game state, broadcasts the question, waits for answers or timeout,
        evaluates answers, pauses, then moves to the next question.
        Output: None
        """
        with self.lock:
            questions = list(self.game_state.get("questions_order") or self.questions)

        for idx in range(start_idx, len(questions)):
            # A new election can happen at any point (e.g. a higher-ID server joins).
            # If this server lost leadership mid-game, stop immediately — the new
            # Quiz Master will call _continue_game and take over.
            if not self._is_quiz_master():
                print(f"[Quiz] No longer the Quiz Master — stopping game leadership.")
                return

            question = questions[idx]

            with self.lock:
                self.game_state["phase"]                = "question"
                self.game_state["current_question_idx"] = idx
                self.game_state["current_question"]     = question
                self.game_state["answers_this_round"]   = {}
                self.game_state["question_start_time"]  = time.time()

            print(f"\n[Quiz] Question {idx+1}/{len(questions)}: {question['question']}")

            self._multicast_to_players({
                "type": "new_question",
                "question_number": idx + 1,
                "total_questions": len(questions),
                "question": question["question"],
                "time_limit": QUESTION_SEC
            })
            self._sync_to_backups()

            # Wait until time is up OR all players have answered
            start = time.time()
            while time.time() - start < QUESTION_SEC:
                with self.lock:
                    num_players  = len(self.game_state["players"])
                    num_answered = len(self.game_state["answers_this_round"])
                if num_players > 0 and num_answered >= num_players:
                    print(f"[Quiz] All players have answered!")
                    break
                time.sleep(0.3)

            self._evaluate(question)

            if not self._is_quiz_master():
                return

            time.sleep(PAUSE_SEC)

        self._finish_game()

    def _continue_game(self):
        """
        The recovery strategy for a crashed Quiz Master: called when a new Quiz Master
        is elected during a running game. Continues the game from the last known
        question index instead of restarting from scratch.
        Input: None
        Calculation:
        Notifies players about the FAILOVER and resumes _play_questions from the current index.
        This is only possible because of the passive replication in _sync_to_backups() — the
        new primary already has a full copy of game_state, so no state is lost (a key benefit
        of state-transfer replication over having no replication / a cold standby).
        Output: None
        """
        idx       = self.game_state["current_question_idx"]
        questions = self.game_state.get("questions_order") or []

        if not questions or idx < 0:
            # No recoverable question order — reset cleanly so clients can rejoin
            print(f"[Quiz] Incomplete game state — resetting to lobby for new game")
            with self.lock:
                self.game_state = {
                    "phase": "lobby",
                    "players": {},
                    "current_question_idx": -1,
                    "current_question": None,
                    "answers_this_round": {},
                    "question_start_time": 0,
                    "questions_order": []
                }
            self._sync_to_backups()
            return

        # If results were already shown for idx, start the NEXT question
        start_idx = idx if self.game_state.get("phase") == "question" else idx + 1

        if start_idx >= len(questions):
            self._finish_game()
            return

        print(f"[Quiz] Continuing game from question {start_idx + 1}")

        # Set phase to "results" so that clients who rejoin in response to
        # server_failover receive a clean "reconnected" without stale q_data.
        # _play_questions will set the correct phase when it broadcasts the question.
        with self.lock:
            self.game_state["phase"] = "results"

        with self.lock:
            total = len(self.game_state.get("questions_order") or [])

        self._multicast_to_players({
            "type": "server_failover",
            "message": f"Quiz Master has failed — new Quiz Master takes over!\n   Repeating question {start_idx + 1}/{total}...",
            "leader_port": self.port,
            "leader_host": self._get_own_host()
        })

        # Give clients 2 seconds to receive the server_failover message and
        # update their server_port before the first new question is sent.
        time.sleep(2)
        self._play_questions(start_idx=start_idx)

    def _evaluate(self, question):
        """
        Evaluates player answers and updates scores.
        Input: question (dict)
        Calculation:
        Compares each player's answer with the correct one, awards points,
        builds a leaderboard, and broadcasts the results.
        Output: None
        """
        with self.lock:
            self.game_state["phase"] = "results"
            answers     = dict(self.game_state["answers_this_round"])
            correct     = question["answer"]

            for uid, answer in answers.items():
                if uid in self.game_state["players"] and answer == correct:
                    self.game_state["players"][uid]["score"] += 1

            answers_by_name = {
                self.game_state["players"][uid]["name"]: answer
                for uid, answer in answers.items()
                if uid in self.game_state["players"]
            }

            leaderboard = sorted(
                [(info["name"], info["score"]) for info in self.game_state["players"].values()],
                key=lambda x: x[1],
                reverse=True
            )

        print(f"[Quiz] Correct answer: {'TRUE' if correct else 'FALSE'}")
        print(f"[Quiz] Current leaderboard:")
        for i, (name, score) in enumerate(leaderboard, 1):
            print(f"        {i}. {name}: {score} Points")

        self._multicast_to_players({
            "type": "question_result",
            "correct_answer": correct,
            "explanation": question["explanation"],
            "your_answers": answers_by_name,
            "leaderboard": leaderboard
        })
        self._sync_to_backups()

    def _finish_game(self):
        """
        Ends the game, announces the winner, and resets to lobby after a pause.
        Input: None
        Calculation:
        Broadcasts the final leaderboard, then resets game_state to lobby after 10 seconds.
        Output: None
        """
        with self.lock:
            self.game_state["phase"] = "finished"
            leaderboard = sorted(
                [(info["name"], info["score"]) for info in self.game_state["players"].values()],
                key=lambda x: x[1],
                reverse=True
            )

        print(f"\n[Quiz] Game finished!")
        print(f"[Quiz] Final leaderboard:")
        for i, (name, score) in enumerate(leaderboard, 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "  "
            print(f"        {medal} {i}. {name}: {score} Points")

        self._multicast_to_players({
            "type": "game_over",
            "leaderboard": leaderboard
        })
        self._sync_to_backups()

        time.sleep(10)
        do_sync = False
        with self.lock:
            if self.game_state["phase"] == "finished":
                self.game_state = {
                    "phase": "lobby",
                    "players": {},
                    "current_question_idx": -1,
                    "current_question": None,
                    "answers_this_round": {},
                    "question_start_time": 0,
                    "questions_order": []
                }
                print(f"[Quiz] Game reset — waiting for new players...")
                do_sync = True
        if do_sync:
            self._sync_to_backups()

    # ─────────────────────────────────────────
    # 8. HELPER METHODS
    # ─────────────────────────────────────────

    def _is_quiz_master(self):
        """
        Returns True if this server is the current Quiz Master.
        Input: None
        Output: bool
        """
        return self.leader_id == self.server_id

    def _multicast_to_players(self, message):
        """
        Sends a message to all connected players. Despite "multicast" in the project
        description, this is application-level GROUP COMMUNICATION / fan-out over plain TCP
        UNICAST — one send_msg() per player in a loop, not a real IP MULTICAST socket/group
        address (no IGMP group join, no single "send once, subnet delivers to many" primitive).
        Input: message (dict)
        Output: None
        """
        with self.lock:
            players_copy = dict(self.game_state["players"])
        for _, info in players_copy.items():
            send_msg(info["host"], info["port"], message)

    def _sync_to_backups(self):
        """
        Sends the current game state to all backup servers.
        This is PASSIVE REPLICATION (the PRIMARY-BACKUP PROTOCOL): only the primary/leader ever
        computes new state; backups are dumb followers that just overwrite their copy — NOT
        active replication, where every replica would independently execute the same operations.
        It is also a full STATE TRANSFER (whole game_state resent each time) rather than an
        OPERATION/DELTA TRANSFER (sending just the diff) — simpler, at the cost of more bandwidth.
        Input: None
        Calculation:
        A deep copy via JSON serialization (MARSHALLING) is used so that subsequent changes to
        game_state in other threads do not corrupt the message already being sent.
        Backups replace their local state with this snapshot, ensuring they can
        take over instantly if the Quiz Master fails (zero data loss failover).
        Output: None
        """
        with self.lock:
            state_copy = json.loads(json.dumps(self.game_state))  # deep copy
            peers_copy = dict(self.peers)
        for _, info in peers_copy.items():
            send_msg(info["host"], info["port"], {
                "type": "state_sync",
                "game_state": state_copy
            })

    def _get_own_host(self):
        """
        Returns the local IP address of this server.
        Input: None
        Output: str
        """
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        except Exception:
            return "127.0.0.1"
        finally:
            s.close()


if __name__ == "__main__":
    QuizServer().run()
