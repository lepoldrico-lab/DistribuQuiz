"""
DistribuQuiz - Client (Player)
================================
Connects to the Quiz Master and participates in the quiz.

Communication model:
  - The client scans server ports (TCP) and asks each one "who_is_leader?" until
    it finds the Quiz Master.  It then sends all game messages (join, answer) to
    that Quiz Master via TCP.
  - The server pushes events (questions, results, failover) back to the client by
    connecting to the CLIENT's own TCP listen port (reverse connection).  The client
    therefore runs its own small TCP server in a background thread.
  - A silence watchdog runs in a second background thread.  If no message arrives
    for SILENCE_TIMEOUT seconds (Quiz Master likely crashed), it searches for the
    new Quiz Master and rejoins automatically.

Start:  python3 client.py
"""

import socket
import threading
import json
import time
import sys

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

BASE_PORT        = 50100   # Servers run on ports 50100, 50101, ...
CLIENT_BASE_PORT = 60000   # Clients run on ports 60000, 60001, ...
SILENCE_TIMEOUT  = 25      # Seconds without server message before trying to reconnect


# ─────────────────────────────────────────────
# MAIN CLASS: Client
# ─────────────────────────────────────────────

class QuizClient:

    # ─────────────────────────────────────────
    # 1. INITIALIZATION
    # ─────────────────────────────────────────

    def __init__(self):
        """
        Initializes the Quiz Client.
        Sets up player name, port, server connection info, and state flags.
        """
        self.player_name      = None
        self.client_port      = self._find_free_port()
        self.has_answered     = False

        self.server_host      = None
        self.server_port      = None
        self.current_question = None

        self.connected         = False
        self.game_over         = False
        self.last_message_time = time.time()

    def _find_free_port(self):
        """
        Searches for a free port starting at CLIENT_BASE_PORT.
        Input: None
        Calculation: Tries to bind ports CLIENT_BASE_PORT to CLIENT_BASE_PORT+100.
        Output: int (free port number)
        """
        for p in range(CLIENT_BASE_PORT, CLIENT_BASE_PORT + 100):
            try:
                test = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                test.bind(("", p))
                test.close()
                return p
            except OSError:
                continue
        raise RuntimeError("No free port found!")

    def get_local_ip(self):
        """
        Returns the local IP address of the client.
        Input: None
        Output: str
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        except Exception:
            return "127.0.0.1"
        finally:
            s.close()

    # ─────────────────────────────────────────
    # 2. STARTUP — entry point
    # ─────────────────────────────────────────

    def run(self):
        """
        Starts the Quiz Client.
        Input: None
        Calculation:
        Prompts for player name, finds the Quiz Master, starts background threads,
        joins the game, and enters the answer input loop.
        Output: None
        """
        print("\n" + "="*55)
        print("   DistribuQuiz — Player Client")
        print("="*55)

        # Step 1: Enter player name
        while True:
            name = input("\nHow do you want to be called? ").strip()
            if name and len(name) <= 20:
                self.player_name = name
                break
            print("   Please enter a valid name (max. 20 characters).")

        # Step 2: Find the Quiz Master
        print("\nSearching for Quiz Master...")
        self.server_port = self.find_quiz_master()

        if not self.server_port:
            print("No Quiz Master found!")
            print("   Make sure at least one server.py is running.")
            sys.exit(1)

        print(f"Quiz Master found on port {self.server_port}")

        # Step 3: Start background threads
        threading.Thread(target=self.listen_for_messages, daemon=True).start()
        threading.Thread(target=self._watch_for_silence,  daemon=True).start()
        time.sleep(0.3)  # Give the listener thread time to bind and start accepting

        # Step 4: Join the quiz
        print(f"\nJoining the quiz as '{self.player_name}'...")
        self.send_to_server({
            "type": "join_game",
            "client_host": self.get_local_ip(),
            "player_name": self.player_name,
            "client_port": self.client_port
        })

        # Step 5: Wait for server confirmation
        wait = 0
        while not self.connected and wait < 5:
            time.sleep(0.5)
            wait += 0.5

        if not self.connected:
            print("No response from Quiz Master.")
            sys.exit(1)

        self.last_message_time = time.time()  # Start silence watchdog timer

        # Step 6: Enter answer input loop (main thread)
        try:
            self.enter_answer()
        except KeyboardInterrupt:
            print("\n\nGoodbye!")

    # ─────────────────────────────────────────
    # 3. FIND THE QUIZ MASTER
    # ─────────────────────────────────────────

    def find_quiz_master(self):
        """
        Queries all known server ports to find the current Quiz Master.
        Input: None
        Calculation:
        Tries ports BASE_PORT to BASE_PORT+20, sends "who_is_leader",
        and returns the leader's port if found.
        Output: int (leader port) or None
        """
        for port in range(BASE_PORT, BASE_PORT + 20):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(1)
                    s.connect(("127.0.0.1", port))
                    s.sendall(json.dumps({"type": "who_is_leader"}).encode())
                    resp = s.recv(1024)
                    data = json.loads(resp.decode())
                    if data.get("leader_port"):
                        self.server_host = data.get("leader_host", "127.0.0.1")
                        return data["leader_port"]
            except Exception:
                continue
        return None

    # ─────────────────────────────────────────
    # 4. SEND MESSAGES TO THE SERVER
    # ─────────────────────────────────────────

    def send_to_server(self, data):
        """
        Sends a JSON message to the Quiz Master.
        Input: data (dict)
        Calculation: Opens a TCP connection to the server and sends the JSON-encoded data.
        Output: bool (True if successful)
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(3)
                s.connect((self.server_host, self.server_port))
                s.sendall(json.dumps(data).encode())
            return True
        except Exception:
            return False

    # ─────────────────────────────────────────
    # 5. RECEIVE MESSAGES FROM THE SERVER
    # ─────────────────────────────────────────

    def listen_for_messages(self):
        """
        Listens for incoming messages from the server on the client's port.
        Input: None
        Calculation:
        Creates a TCP socket on client_port and for each connection
        decodes the JSON and passes it to _handle_server_message.
        Output: None
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", self.client_port))  # 0.0.0.0 = all interfaces, so the server
        sock.listen(5)                              # can reach us over the local network, not just localhost

        while True:
            try:
                conn, _ = sock.accept()
                conn.settimeout(3)
                data = conn.recv(16384)
                conn.close()
                if not data:
                    continue
                msg = json.loads(data.decode())
                self._handle_server_message(msg)
            except Exception:
                continue

    def _handle_server_message(self, msg):
        """
        Reacts to messages from the server and updates client state.
        Input: msg (dict)
        Calculation:
        Dispatches each message type to the correct UI output or state update.
        Output: None
        """
        self.last_message_time = time.time()
        t = msg.get("type")

        if t == "joined":
            # Successfully joined the lobby
            self.connected = True
            print(f"\n{msg['message']}")
            print(f"Waiting for more players...\n")

        elif t == "reconnected":
            # Successfully reconnected after a server failover
            self.connected = True
            print(f"\n{msg['message']}")
            if "current_question" in msg:
                self.current_question = msg["current_question"]
                self.has_answered = False
                self._show_question(msg["current_question"])
            else:
                print(f"Waiting for the next question...\n")

        elif t == "join_failed":
            # Server rejected the join (e.g. name already taken)
            print(f"\nJoining failed: {msg['reason']}")
            sys.exit(1)

        elif t == "redirect":
            # This server is a backup — connect to the real Quiz Master instead
            self.server_port = msg["leader_port"]
            self.server_host = msg.get("leader_host", self.server_host)
            print(f"\nConnecting to new Quiz Master on port {self.server_port}")
            self.send_to_server({
                "type": "join_game",
                "client_host": self.get_local_ip(),
                "player_name": self.player_name,
                "client_port": self.client_port
            })

        elif t == "player_joined":
            # Broadcast: another player has joined the lobby
            print(f"👤 '{msg['player_name']}' joined (total: {msg['total_players']} players)")

        elif t == "new_question":
            # A new question has arrived
            self.current_question = msg
            self.has_answered = False
            self._show_question(msg)

        elif t == "question_result":
            # The quiz master evaluated the last question
            self._show_result(msg)

        elif t == "server_failover":
            # The Quiz Master crashed — a new one took over
            print(f"\n{msg['message']}")
            if msg.get("leader_port"):
                self.server_port = msg["leader_port"]
                self.server_host = msg.get("leader_host", self.server_host)
                print(f"   Connecting to new Quiz Master on port {self.server_port}...")
            print(f"   Game will continue...\n")

        elif t == "game_over":
            # The game has ended
            self._show_final_standings(msg)
            self.game_over = True

    # ─────────────────────────────────────────
    # 6. DISPLAY — show questions and results
    # ─────────────────────────────────────────

    def _show_question(self, msg):
        """
        Displays a new question to the player.
        Input: msg (dict)
        Output: None
        """
        print("\n" + "="*55)
        print(f"  Question {msg['question_number']}/{msg['total_questions']}")
        print("="*55)
        print(f"\n  {msg['question']}\n")
        print(f"  You have {msg['time_limit']} seconds!")
        print(f"\n  Press [w] for TRUE or [f] for FALSE and press Enter")
        print("="*55)

    def _show_result(self, msg):
        """
        Displays the result of the last question.
        Input: msg (dict)
        Output: None
        """
        correct   = msg["correct_answer"]
        my_answer = msg["your_answers"].get(self.player_name)

        print("\n" + "─"*55)
        print(f"  SOLUTION")
        print("─"*55)
        print(f"  Correct Answer: {'TRUE' if correct else 'FALSE'}")

        if my_answer is None:
            print(f"  Your Answer:    Too late / not answered")
        elif my_answer == correct:
            print(f"  Your Answer:    Correct! (+1 Point)")
        else:
            print(f"  Your Answer:    Wrong")

        print(f"\n  {msg['explanation']}")

        print(f"\nCurrent Leaderboard:")
        for i, (name, score) in enumerate(msg["leaderboard"], 1):
            marker = " ← YOU" if name == self.player_name else ""
            print(f"     {i}. {name}: {score} Points{marker}")
        print("─"*55)

    def _show_final_standings(self, msg):
        """
        Displays the final standings at the end of the game.
        Input: msg (dict)
        Output: None
        """
        print("\n" + "="*55)
        print(f"  GAME OVER!")
        print("="*55)
        print(f"\n  FINAL SCORE:\n")
        for i, (name, score) in enumerate(msg["leaderboard"], 1):
            medal  = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "  "
            marker = " ← YOU" if name == self.player_name else ""
            print(f"     {medal} {i}. {name}: {score} Points{marker}")

        if msg["leaderboard"]:
            winner, points = msg["leaderboard"][0]
            print(f"\n  Winner: {winner} with {points} Points!")

        print("="*55)
        print(f"\n  Press Enter to exit...")

    # ─────────────────────────────────────────
    # 7. ANSWER INPUT — main game loop
    # ─────────────────────────────────────────

    def enter_answer(self):
        """
        Waits for player input and sends answers to the server.
        Input: None
        Calculation:
        Reads stdin, validates input (w=TRUE, f=FALSE), and sends the answer to the server.
        Output: None
        """
        while not self.game_over:
            try:
                raw = input().strip().lower()
            except EOFError:
                break

            if self.game_over:
                break

            if self.current_question is None or self.has_answered:
                continue

            if raw in ["w", "wahr", "true", "t"]:
                answer = True
            elif raw in ["f", "falsch", "false"]:
                answer = False
            else:
                print(f"  Please enter [w] for TRUE or [f] for FALSE.")
                continue

            success = self.send_to_server({
                "type": "submit_answer",
                "player_name": self.player_name,
                "answer": answer
            })

            if success:
                self.has_answered = True
                print(f"  Your answer '{'TRUE' if answer else 'FALSE'}' has been sent.")
                print(f"     Waiting for other players...")
            else:
                print(f"  Answer could not be sent!")

    # ─────────────────────────────────────────
    # 8. RECONNECT / FAULT TOLERANCE
    # ─────────────────────────────────────────

    def _watch_for_silence(self):
        """
        Monitors for server silence and triggers reconnect if needed.
        Input: None
        Calculation:
        Every 3 seconds, checks if time since last server message exceeds SILENCE_TIMEOUT.
        SILENCE_TIMEOUT (25 s) is intentionally larger than the server's TIMEOUT_SEC (15 s)
        plus election time (~2 s), so the client waits long enough for a new Quiz Master to
        be elected and resume the game before giving up and searching itself.
        Output: None
        """
        while not self.game_over:
            time.sleep(3)
            if not self.connected or self.game_over:
                continue
            if time.time() - self.last_message_time > SILENCE_TIMEOUT:
                print(f"\nNo response from server. Searching for a new Quiz Master...")
                self._try_reconnect()

    def _try_reconnect(self):
        """
        Attempts to find a new Quiz Master and rejoin the game.
        Input: None
        Calculation:
        Calls find_quiz_master, then sends a join_game message to the new server.
        Output: None
        """
        new_port = self.find_quiz_master()
        if not new_port:
            print(f"No Quiz Master available. Trying again later...")
            self.last_message_time = time.time()
            return
        self.server_port = new_port
        ok = self.send_to_server({
            "type": "join_game",
            "client_host": self.get_local_ip(),
            "player_name": self.player_name,
            "client_port": self.client_port
        })
        if ok:
            print(f"Reconnecting to Quiz Master on port {new_port}...")
        else:
            print(f"Reconnection failed.")
        self.last_message_time = time.time()


if __name__ == "__main__":
    QuizClient().run()
