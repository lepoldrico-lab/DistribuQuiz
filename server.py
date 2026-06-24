"""
DistribuQuiz - Server
======================
Distributed Quiz-System: Multiple Servers working together.
One of them is Quiz Master (Leader), the others are Backups.
When the Quiz Master fails, an Backup takes over automatically.

Start:  python3 server.py
"""

import socket
import threading
import json
import time
import random
import os
import uuid

# ─────────────────────────────────────────────
# KONFIGURATION
# ─────────────────────────────────────────────

DISCOVERY_PORT  = 55000       # Broadcast Port for Server-Discovery
BASE_PORT       = 50100       # Server running on Ports 50100, 50101, ...
HEARTBEAT_SEC   = 2           # Every 2 Sek: "I'm alive!"
TIMEOUT_SEC     = 15          # After 15 Sek without signal: Server failed
QUESTION_SEC    = 10          # How long players have per question
PAUSE_SEC       = 3           # Pause between questions (showing solution)
MIN_PLAYERS     = 2           # Minimum number of players to start
QUESTIONS_FILE  = "questions.json" # File with the questions (JSON)


# ─────────────────────────────────────────────
# Helper Functions: Send Messages
# ─────────────────────────────────────────────

def send_msg(host, port, data: dict):
    """
    Sends a JSON message to another server or client.
    Input: host (str), port (int), data (dict)
    Calculation: 
    Opens a TCP connection, sends the JSON-encoded data, and closes the connection.
    Output: bool (True if sent successfully, False otherwise)
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(3)                     # Set a timeout for the connection
            s.connect((host, port))             # Connect to the specified host and port
            s.sendall(json.dumps(data).encode())# Send the JSON-encoded data
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────
# Main-CLASS: Server
# ─────────────────────────────────────────────

class QuizServer:
    def __init__(self):
        """
        Initializes the Quiz Server.
        Input: None
        Calculation:
        Generates a unique server ID, finds a free port, initializes leader info, peer list, and game state.
        Output: None
        """
        self.server_id = str(uuid.uuid4())
        self.port        = self._find_free_port()

        self.leader_id   = None
        self.leader_port = None

        self.peers       = {}      # {server_id: {port, last_seen}}
        self.lock        = threading.Lock()

        # Quiz-Game State (Backups included copies!)
        self.game_state = {
            "phase": "lobby",          # lobby | question | results | finished
            "players": {},             # {player_name: {port, score}}
            "current_question_idx": -1,# Index of the current question in the questions_order list
            "current_question": None,  # The current question being asked
            "answers_this_round": {},  # {player_name: answer}
            "question_start_time": 0,  # Timestamp when the current question was sent
            "questions_order": []      # Mashed Order — sync on backups
        }

        self.questions = self._load_questions()

        print(f"\n{'='*55}")
        print(f"  DistribuQuiz Server is running!")
        print(f"  Server-ID : {self.server_id}")
        print(f"  Port      : {self.port}")
        print(f"  Questions    : {len(self.questions)} loaded")
        print(f"{'='*55}\n")

    def _find_free_port(self):
        """
        Is searching for a free Port in BASE_PORT.
        Input: None
        Calculation: 
        Scans Ports from BASE_PORT to BASE_PORT + 20 and returns the first free port.
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
        raise RuntimeError("No free Port was found!")

    def _load_questions(self):
        """
        Loads the questions from the JSON file.
        Input: None
        Calculation: 
        Reads the questions from the JSON file and returns them.
        Output: list (questions loaded from the file)
        """
        if not os.path.exists(QUESTIONS_FILE):
            print(f"Error: {QUESTIONS_FILE} not found!")
            return []
        with open(QUESTIONS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)

    # ─────────────────────────────────────────
    # DYNAMIC DISCOVERY
    # ─────────────────────────────────────────

    def broadcast_presence(self):
        """
        Broadcasts the server's presence to the network for discovery.
        Input: None
        Calculation:
        Sends a UDP broadcast message to the DISCOVERY_PORT and listens for responses from other servers.
        Output: None
        """
        print("[Discovery] Searching for Servers in the Network...")

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.settimeout(2)

        discovery_msg = {
            "type": "discover_server" # Message Type
        }

        sock.sendto( # Send Discovery Message
            json.dumps(discovery_msg).encode(), # Encode Discovery Message as JSON
            ("255.255.255.255", DISCOVERY_PORT) # Broadcast to all devices on the network at the DISCOVERY_PORT
        )

        found = 0 # Count of found servers

        # Listen for responses from other servers
        # We will listen for a short period to gather responses from other servers.
        while True:
            try:
                data, addr = sock.recvfrom(4096) # Receive data from the socket (up to 4096 bytes)
                msg = json.loads(data.decode()) # Decode the received data as JSON

                sid = msg["server_id"]

                if sid == self.server_id:
                    continue

                with self.lock:
                    # If the server is already known, update its last_seen timestamp; otherwise, add it to the peers list with its port, host, and last seen timestamp.
                    self.peers[sid] = {
                        "port": msg["port"],
                        "host": addr[0],
                        "last_seen": time.time()
                    }

                found += 1

                print(
                    f"[Discovery] Server found: "
                    f"ID={sid} "
                    f"IP={addr[0]} "
                    f"Port={msg['port']}"
                )

            except socket.timeout:
                break

        print(f"[Discovery] {found} Server found")

    # ─────────────────────────────────────────
    # VOTING: Bully-Algorithmn
    # It is decided by the Server-ID (UUID) which Server is the new Quiz Master.
    # ─────────────────────────────────────────

    def start_election(self):
        """
        Voting: Server with highest ID becomes Quiz Master.
        Input: None
        Calculation:
        Collects all server IDs (including self), determines the highest ID, and sets it as the new leader.
        Output: None
        """
        print(f"\n[Voting] Election started! My ID: {self.server_id}")

        with self.lock:
            all_ids = {self.server_id: self.port}
            for sid, info in self.peers.items():
                all_ids[sid] = info["port"]

        # Determine the server with the highest ID (UUID) to become the new Quiz Master.
        winner_id   = max(all_ids.keys())
        winner_port = all_ids[winner_id]

        # If the winner is this server, it becomes the new Quiz Master; otherwise, it updates its leader information.
        was_leader_before = (self.leader_id == self.server_id)
        self.leader_id   = winner_id
        self.leader_port = winner_port

        # If this server is the new Quiz Master, print a message and take over the game if it was running.
        if winner_id == self.server_id:
            print(f"[Voting] I'm the new Quiz Master! (ID: {self.server_id})")
            if not was_leader_before:
                # Freshly chosen - if a game is running, take it over
                if self.game_state["phase"] in ["question", "results"]:
                    print(f"[Voting] I'm taking over the running game from the failed server!")
                    threading.Thread(target=self._continue_game, daemon=True).start()
        else:
            print(f"[Voting] Quiz Master chosen: Server {winner_id} (Port {winner_port})")

        with self.lock:
            peers_copy = dict(self.peers)

        # Notify all peers about the new leader by sending a "new_leader" message with the winner's ID and port.
        for sid, info in peers_copy.items():
            send_msg(info["host"], info["port"], {
                "type": "new_leader",
                "leader_id": winner_id,
                "leader_port": winner_port
            })

    # ─────────────────────────────────────────
    # FAULT TOLERANCE: Heartbeat
    # Sends periodic heartbeat messages to all peers to detect failures.
    # ─────────────────────────────────────────

    def send_heartbeats(self):
        """
        Sending every 2 Seconds 'I'm alive!' on all Peers.
        Input: None
        Calculation:
        Every HEARTBEAT_SEC seconds, sends a heartbeat message to all known peers.
        Output: None
        """
        while True:
            time.sleep(HEARTBEAT_SEC)
            with self.lock:
                peers_copy = dict(self.peers)
            # Send heartbeat messages to all known peers with the server's ID and port.
            for sid, info in peers_copy.items():
                send_msg(info["host"], info["port"], {
                    "type": "heartbeat",        # Message Type 
                    "server_id": self.server_id,# Send Server ID
                    "port": self.port           # Send Port
                })

    def check_for_failures(self):
        """
        Checks regularly if servers have failed. 
        Input: None
        Calculation:
        Every HEARTBEAT_SEC seconds, checks the last_seen timestamp of each peer.
        If a peer has not sent a heartbeat within TIMEOUT_SEC, it is considered failed and
        removed from the peer list. If the failed server was the Quiz Master, a new election is started.
        Output: None
        """
        while True:
            time.sleep(HEARTBEAT_SEC)
            now = time.time()
            failed = []

            # Check for failed servers
            with self.lock:
                for sid, info in self.peers.items():
                    if now - info["last_seen"] > TIMEOUT_SEC:
                        failed.append(sid)

            # Remove failed servers and handle Quiz Master failure
            for sid in failed:
                with self.lock:
                    self.peers.pop(sid, None)
                print(f"\n[Fault Tolerance] Server {sid} failed!")

                # If the Quiz Master failed → New election!
                if sid == self.leader_id:
                    print(f"[Fault Tolerance] Quiz Master failed! Starting new election...")
                    self.leader_id = None
                    threading.Thread(target=self.start_election, daemon=True).start()

    # ─────────────────────────────────────────
    # QUIZ-LOGIC (only the Quiz Master is handling it actively)
    # ─────────────────────────────────────────

    def _is_quiz_master(self):
        """
        Checks if this server is the current Quiz Master.
        Input: None
        Calculation: Compares the server's ID with the leader_id.
        Output: bool
        """
        return self.leader_id == self.server_id

    def _broadcast_to_players(self, message):
        """
        Is sending a Message to all connected Players.
        Input: message (dict)
        Calculation: Iterates through the players in the game state and sends the message to each player's host and port.
        Output: None
        """
        with self.lock:
            players_copy = dict(self.game_state["players"])
        # Send the message to all players in the game state by iterating through the players_copy dictionary and using the send_msg function to send the message to each player's host and port.
        for name, info in players_copy.items():
            send_msg(info["host"], info["port"], message)

    def _sync_to_backups(self):
        """
        Sends the current game state to all backup servers.
        Input: None
        Calculation: Creates a deep copy of the game state and sends it to each peer server.
        Output: None
        """
        with self.lock:
            state_copy = json.loads(json.dumps(self.game_state))
            peers_copy = dict(self.peers)
        for sid, info in peers_copy.items():
            send_msg(info["host"], info["port"], {
                "type": "state_sync",
                "game_state": state_copy
            })

    def run_quiz(self):
        """
        Starts the quiz once enough players are in the lobby.
        Input: None
        Calculation:
        Continuously checks if this server is the Quiz Master and if the game is in the lobby phase.
        If there are enough players, it starts the game.
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

            if num_players >= MIN_PLAYERS: # If enough players are ready, start the game
                print(f"\n[Quiz] {num_players} Players ready. Starting Game!")
                self._start_game()

    def _start_game(self):
        """
        Starts the quiz by playing all questions in a random order.
        Input: None
        Calculation:
        Shuffles the questions, updates the game state, and calls _play_questions to start the quiz.
        Output: None
        """
        shuffled = list(self.questions)
        random.shuffle(shuffled)
        with self.lock:
            self.game_state["questions_order"] = shuffled
        self._play_questions(start_idx=0)

    def _get_own_host(self):
        """
        Returns the local IP address of the server.
        Input: None
        Calculation:
        Creates a UDP socket, connects to a public DNS server (8.8.8.8) and retrieves the local IP address used for that connection.
        Output: str (local IP address)
        """
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        except Exception:
            return "127.0.0.1"
        finally:
            s.close()

    def _continue_game(self):
        """
        Is called when a new Quiz Master is elected.
        It continues the game from the last question index.
        Input: None
        Calculation:
        Retrieves the current question index from the game state and calls _play_questions to continue the quiz.
        Output: None
        """
        idx = self.game_state["current_question_idx"]
        questions = self.game_state.get("questions_order") or []
        if idx < 0 or idx >= len(questions):
            return

        print(f"[Quiz] Continue Game from Question {idx+1}")

        self._broadcast_to_players({
            "type": "server_failover",
            "message": "Quiz Master has failed — new Quiz Master takes over!",
            "leader_port": self.port,
            "leader_host": self._get_own_host()
        })

        time.sleep(2)
        self._play_questions(start_idx=idx)

    def _play_questions(self, start_idx):
        """
        Plays all questions starting from start_idx.
        Input: start_idx (int)
        Calculation:
        Iterates through the questions starting from start_idx, updates the game state for each question,
        broadcasts the question to players, waits for answers or timeout, evaluates the answers, and pauses before the next question.
        Output: None
        """
        with self.lock:
            questions = list(self.game_state.get("questions_order") or self.questions)

        for idx in range(start_idx, len(questions)):
            if not self._is_quiz_master():
                print(f"[Quiz] I'm not the Quiz Master anymore — ending game leadership.")
                return

            question = questions[idx]

            # Update game state for the new question
            with self.lock:
                self.game_state["phase"] = "question"
                self.game_state["current_question_idx"] = idx
                self.game_state["current_question"] = question
                self.game_state["answers_this_round"] = {}
                self.game_state["question_start_time"] = time.time()

            print(f"\n[Quiz] Question {idx+1}/{len(questions)}: {question['question']}")

            # Broadcast the question to all players
            self._broadcast_to_players({
                "type": "new_question",             # Message Type
                "question_number": idx + 1,         # Question Number (1-based)
                "total_questions": len(questions),  # Total Questions
                "question": question["question"],         # Question Text
                "time_limit": QUESTION_SEC          # Time Limit for Answering
            })
            # Sync the game state to backups after broadcasting the question
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

    def _evaluate(self, question):
        """
        Evaluates the answers and assigns points.
        Input: question (dict) - the current question
        Calculation:
        Compares the players' answers with the correct answer, updates scores, and broadcasts the results and leaderboard.
        Output: None
        """
        with self.lock:
            self.game_state["phase"] = "results"
            answeren = dict(self.game_state["answers_this_round"])
            korrekt = question["answer"]
            
            # Update scores for players who answered correctly
            for uid, answer in answeren.items():
                if uid in self.game_state["players"]:
                    if answer == korrekt:
                        self.game_state["players"][uid]["score"] += 1

            # Create a mapping of player names to their answers for broadcasting
            answeren_by_name = {
                self.game_state["players"][uid]["name"]: answer
                for uid, answer in answeren.items()
                if uid in self.game_state["players"]
            }
            
            # Create a sorted leaderboard based on scores
            rangliste = sorted(
                [
                    (info["name"], info["score"])
                    for info in self.game_state["players"].values()
                ],
                key=lambda x: x[1],
                reverse=True
            )

        print(f"[Quiz] Correct Answer: {'TRUE' if korrekt else 'FALSE'}")
        print(f"[Quiz] Current Leaderboard:")
        # Print the leaderboard with positions and scoresW
        for i, (name, score) in enumerate(rangliste, 1):
            print(f"        {i}. {name}: {score} Points")
        # Broadcast the results to all players
        self._broadcast_to_players({
            "type": "question_result",              # Message Type
            "correct_answer": korrekt,              # Correct Answer
            "explanation": question["explanation"],  # Explanation for the answer
            "your_answers": answeren_by_name,      # Players' Answers
            "leaderboard": rangliste                # Current Leaderboard
        })
        # Sync the updated game state to backups after evaluation
        self._sync_to_backups()

    def _finish_game(self):
        """
        Ends the game and crowns the winner.
        Input: None
        Calculation:
        Updates the game state to finished, sorts the players by score, broadcasts the final leaderboard, and resets the game state after a pause.
        Output: None
        """
        with self.lock:
            self.game_state["phase"] = "finished"
            rangliste = sorted(
                [(info["name"], info["score"]) for info in self.game_state["players"].values()],
                key=lambda x: x[1],
                reverse=True
            )

        print(f"\n[Quiz] Game finished!")
        print(f"[Quiz] Final Leaderboard:")
        for i, (name, score) in enumerate(rangliste, 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "  "
            print(f"        {medal} {i}. {name}: {score} Points")

        self._broadcast_to_players({
            "type": "game_over",
            "leaderboard": rangliste
        })
        self._sync_to_backups()

        time.sleep(10)
        do_sync = False
        with self.lock:
            if self.game_state["phase"] == "finished":
                self.game_state = {
                    "phase": "lobby",           # Reset to lobby phase
                    "players": {},              # Reset players list
                    "current_question_idx": -1, # Reset current question index
                    "current_question": None,   # Reset current question
                    "answers_this_round": {},   # Reset answers for the round
                    "question_start_time": 0,   # Reset question start time
                    "questions_order": []       # Reset questions order
                }
                print(f"[Quiz] Game reset — waiting for new players...")
                # Sync to backups after resetting the game state
                do_sync = True
        if do_sync:
            self._sync_to_backups()

    # ─────────────────────────────────────────
    # Retrieve Messages
    # ─────────────────────────────────────────

    def handle_message(self, conn, addr):
        """
        Handles incoming messages from clients or other servers.
        Input: conn (socket), addr (tuple)
        Calculation:
        Sets a timeout for the connection, receives data, decodes the JSON message, and processes it based on its type.
        Output: None
        """
        try:
            conn.settimeout(3)
            data = conn.recv(16384)
            if not data:
                return
            msg = json.loads(data.decode())

            if msg.get("type") == "who_is_leader":
                conn.sendall(json.dumps({
                    "leader_id": self.leader_id,        # Send current leader ID
                    "leader_port": self.leader_port,    # Send current leader port
                    "leader_host": self.peers[self.leader_id]["host"] # Send current leader host (if known)
                }).encode())
                return
            # Process the message based on its type
            self._process(msg, addr[0])
        except Exception:
            pass
        finally:
            conn.close()

    def _process(self, msg, peer_host="127.0.0.1"):
        """
        Processes the received message based on its type.
        Input: msg (dict), peer_host (str)
        Calculation:
        Determines the type of the message and executes the corresponding logic for server-to-server or client-to-server communication.
        Output: None
        """
        t = msg.get("type") # Get Message Type

        # ─── Server to/from Server processing ───
        if t == "hello":
            # New Server discovered: Add to Peers and send Welcome
            with self.lock:
                # Add the new server to the peers list with its port, host, and last seen timestamp
                self.peers[msg["server_id"]] = {
                    "port": msg["port"],
                    "host": peer_host,
                    "last_seen": time.time()
                }
            print(f"[Discovery] New Server: ID={msg['server_id']} Port={msg['port']}")
            # Send a welcome message back to the new server with the current server's ID, port, and leader information
            send_msg(peer_host, msg["port"], {
                "type": "welcome",
                "server_id": self.server_id,
                "port": self.port,
                "leader_id": self.leader_id,
                "leader_port": self.leader_port
            })

        elif t == "welcome":
            # It's not a "hello" message, because this server is already known. It is a "welcome" message from a new server that responded to our "hello".
            # New Server responded to our Hello: Add to Peers and start Election if needed
            with self.lock:
                self.peers[msg["server_id"]] = {
                    "port": msg["port"],
                    "host": peer_host,
                    "last_seen": time.time()
                }
            # If the welcome message contains leader information, update the current leader ID and port
            if msg.get("leader_id"):
                self.leader_id   = msg["leader_id"]
                self.leader_port = msg["leader_port"]
                print(f"[Discovery] Known Quiz Master: Server {self.leader_id}")
            threading.Thread(target=self.start_election, daemon=True).start()

        elif t == "heartbeat":
            # Update last_seen for the sending server
            with self.lock:
                if msg["server_id"] in self.peers: # If the server is already known, update its last_seen timestamp
                    self.peers[msg["server_id"]]["last_seen"] = time.time()
                else: # If the server is not known, add it to the peers list with its port, host, and last seen timestamp
                    self.peers[msg["server_id"]] = {
                        "port": msg["port"],
                        "host": peer_host,
                        "last_seen": time.time()
                    }

        elif t == "new_leader":
            # Update the current Quiz Master information
            self.leader_id   = msg["leader_id"]
            self.leader_port = msg["leader_port"]
            if self.leader_id == self.server_id: # If this server is the new Quiz Master, print a message indicating that it is now the Quiz Master
                print(f"[Wahl] I'm the new Quiz Master!")
            else: # If another server is the new Quiz Master, print a message indicating the new Quiz Master server ID
                print(f"[Wahl] New Quiz Master: Server {self.leader_id}")

        elif t == "state_sync":
            # Update the game state from the Quiz Master
            with self.lock:
                self.game_state = msg["game_state"] # Update the game state with the received game state from the Quiz Master
            phase = self.game_state.get("phase", "?") # Get the current phase of the game from the game state, defaulting to "?" if not found
            num_players = len(self.game_state.get("players", {})) # Get the number of players in the game from the game state, defaulting to 0 if not found
            print(f"[Sync] Game State updated (Phase: {phase}, Players: {num_players})")

        elif t == "join_game":
            # A new player wants to join the game
            if not self._is_quiz_master():
                send_msg("127.0.0.1", msg["client_port"], {
                    "type": "redirect",
                    "leader_port": self.leader_port
                })
                return

            player_name = msg["player_name"]
            client_port = msg["client_port"]
            client_host = msg.get("client_host", "127.0.0.1") # Get the client host from the message, defaulting to "127.0.0.1" if not found

            reject = False
            reconnect_data = None
            num_players = 0

            with self.lock:
                # If the game is finished, reset the game state to lobby and prepare for a new game
                if self.game_state["phase"] == "finished":
                    # Reset the game state to lobby and prepare for a new game
                    self.game_state = {
                        "phase": "lobby",           # Reset to lobby phase
                        "players": {},              # Reset players list
                        "current_question_idx": -1, # Reset current question index
                        "current_question": None,   # Reset current question
                        "answers_this_round": {},   # Reset answers for the round
                        "question_start_time": 0,   # Reset question start time
                        "questions_order": []       # Reset questions order
                    }
                    print(f"[Quiz] New Game Started (Player Connected)")

                existing_uid = None
                # Check if the player name already exists in the game state
                for uid, p in self.game_state["players"].items():
                    if p["name"] == player_name:
                        existing_uid = uid
                        break
                
                # Handle player reconnection or new player joining
                if existing_uid is not None:
                    if self.game_state["phase"] == "lobby":
                        # Reject if the game is in the lobby phase and the name is already taken
                        reject = True
                    else:
                        # Reconnect during running game: update address, restore score
                        self.game_state["players"][existing_uid]["port"] = client_port
                        self.game_state["players"][existing_uid]["host"] = client_host
                        score = self.game_state["players"][existing_uid]["score"]
                        q_data = None
                        # If the game is in the question phase and there is a current question, calculate the remaining time and prepare the question data for reconnection
                        if self.game_state["phase"] == "question" and self.game_state["current_question"]:
                            elapsed = time.time() - self.game_state["question_start_time"]
                            remaining = max(1, int(QUESTION_SEC - elapsed))
                            q_data = {
                                "type": "new_question",                                                            # Message Type
                                "question_number": self.game_state["current_question_idx"] + 1,                    # Question Number (1-based) 
                                "total_questions": len(self.game_state.get("questions_order") or self.questions),  # Total Questions
                                "question": self.game_state["current_question"]["question"],                       # Question Text
                                "time_limit": remaining                                                            # Time Limit for Answering (remaining time)
                            }
                        # If the game is in the results phase and there is a current question, prepare the question result data for reconnection
                        reconnect_data = {"score": score, "q_data": q_data}
                else: # New Player: Add to Game State
                    player_uid = str(uuid.uuid4())
                    # Add the new player to the game state with their name, unique ID, host, port, and initial score
                    self.game_state["players"][player_uid] = {
                        "name": player_name,    # Player Name
                        "uid": player_uid,      # Unique Player ID
                        "port": client_port,    # Client Port
                        "host": client_host,    # Client Host
                        "score": 0              # Initial Score
                    }
                    num_players = len(self.game_state["players"])

            if reject: # Reject the player if the name is already taken in the lobby phase
                send_msg(client_host, client_port, {
                    "type": "join_failed",              # Message Type
                    "reason": "Name already taken."  # Reason for rejection (name already taken)
                })
                return

            if reconnect_data is not None: # Reconnect during running game: Send current score and question data
                payload = {
                    "type": "reconnected",              # Message Type
                    "player_name": player_name,         # Player Name
                    "score": reconnect_data["score"],   # Current Score
                    "message": f"Welcome back, {player_name}! Your score: {reconnect_data['score']} point(s)." # Welcome message with current score
                }
                if reconnect_data["q_data"]: # If there is question data for reconnection, include it in the payload
                    payload["current_question"] = reconnect_data["q_data"] 
                send_msg(client_host, client_port, payload) # Send the reconnection payload to the client
                self._sync_to_backups() # Sync the game state to backups after reconnection
                print(f"[Quiz] Player '{player_name}' reconnected (Score: {reconnect_data['score']})")
                return

            print(f"[Quiz] New Player: '{player_name}' (total: {num_players})")

            # Send welcome message to the new player
            send_msg(
                client_host,
                client_port,
                {
                    "type": "joined",
                    "player_name": player_name,
                    "message": f"Welcome {player_name}!"
                }
            )

            # Broadcast to all players that a new player has joined
            self._broadcast_to_players({
                "type": "player_joined",    # Message Type
                "player_name": player_name, # Player Name of the new player
                "total_players": num_players # Total number of players in the game
            })
            # Sync the game state to backups after a new player has joined
            self._sync_to_backups()

        elif t == "submit_answer": # Player submitted an answer
            if not self._is_quiz_master(): # If this server is not the Quiz Master, redirect the player to the current Quiz Master
                return

            player_name = msg["player_name"]
            answer = msg["answer"]

            with self.lock:
                if self.game_state["phase"] != "question": # If the game is not in the question phase, ignore the answer
                    return
                player_uid = None
                for uid, p in self.game_state["players"].items(): # Find the player UID based on the player name
                    if p["name"] == player_name:
                        player_uid = uid
                        break
                if player_uid is None: # If the player is not found in the game state, ignore the answer
                    return

                self.game_state["answers_this_round"][player_uid] = answer # Store the player's answer for this round in the game state

            print(f"[Quiz] {player_name} has answered: {'TRUE' if answer else 'FALSE'}")

    def discovery_listener(self):
        """
        Listens for UDP broadcasts and responds with own server information.
        Input: None
        Calculation:
        Creates a UDP socket, binds to DISCOVERY_PORT, and continuously listens for discovery messages.
        If a discovery message is received, it responds with its server ID and port.
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
                if msg.get("type") == "discover_server": # If a discovery message is received, respond with server information
                    response = {
                        "type": "server_present",       # Message Type
                        "server_id": self.server_id,    # Server ID
                        "port": self.port               # Server Port
                    }
                    # Send the response back to the sender of the discovery message
                    sock.sendto(json.dumps(response).encode(), addr)
            except Exception:
                pass

    def discovery_loop(self):
        """
        Retry regularly the Discovery, to find new servers.
        Input: None
        Calculation:
        Every 30 seconds, calls broadcast_presence to discover new servers in the network.
        Output: None
        """
        while True:
            time.sleep(30)
            # Retry Discovery
            self.broadcast_presence()

    def listen_for_connections(self):
        """
        Listening on incoming TCP connections.
        Input: None
        Calculation:
        Creates a TCP socket, binds to the server port, and listens for incoming connections.
        Output: None
        """
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind(("0.0.0.0", self.port))
        server_sock.listen(20)
        while True:
            # Accept incoming connections and start a new thread to handle each connection
            conn, addr = server_sock.accept()
            # Start a new thread to handle the incoming connection and process the message
            threading.Thread(target=self.handle_message, args=(conn, addr), daemon=True).start() 

    # ─────────────────────────────────────────
    # START
    # ─────────────────────────────────────────

    def run(self):
        """
        Starts the Quiz Server.
        Input: None
        Calculation:
        Initializes the server, starts threads for listening, heartbeats, failure checking, quiz logic, and discovery.
        If no Quiz Master is found after discovery, starts an election.
        Output: None
        """
        threads = [ # Define Threads
            threading.Thread(target=self.listen_for_connections, daemon=True), # Start listening for incoming TCP connections
            threading.Thread(target=self.send_heartbeats,        daemon=True), # Start sending heartbeats to peers
            threading.Thread(target=self.check_for_failures,     daemon=True), # Start checking for failed servers
            threading.Thread(target=self.run_quiz,               daemon=True), # Start the quiz logic (only if this server is the Quiz Master)
            threading.Thread(target=self.discovery_listener, daemon=True),     # Start listening for UDP discovery messages
            threading.Thread(target=self.discovery_loop, daemon=True),         # Start the discovery loop to periodically broadcast presence and discover new servers
        ]
        for t in threads: # Start Threads
            t.start()

        time.sleep(0.5)
        # Broadcast Presence to find other servers
        self.broadcast_presence()

        time.sleep(4)
        # If no Quiz Master is found after discovery, start an election
        if self.leader_id is None:
            print("[Voting] No Quiz Master found → start election...")
            self.start_election()

        print("\n[Server] Running. Press Ctrl+C to stop.\n")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[Server] Stopping...")


if __name__ == "__main__":
    """
    Main entry point for the Quiz Server.
    Initializes and runs the QuizServer.
    """
    QuizServer().run()