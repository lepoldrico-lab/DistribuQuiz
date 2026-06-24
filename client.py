"""
DistribuQuiz - Client (Player)
================================
Connects to the Quiz Master and participates in the quiz.

Start:  python3 client.py
"""

import socket
import threading
import json
import time
import sys

# ─────────────────────────────────────────────
# KONFIGURATION
# ─────────────────────────────────────────────

BASE_PORT        = 50100      # Server is running on Ports 50100, 50101, ...
CLIENT_BASE_PORT = 60000      # Clients is running on Ports 60000, 60001, ...


# ─────────────────────────────────────────────
# CLIENT-Class
# ─────────────────────────────────────────────

SILENCE_TIMEOUT = 25   # Seconds without server response before trying to reconnect

class QuizClient:
    def __init__(self):
        """
        Initializes the Quiz Client.
        Sets up the necessary attributes for the client, 
        including player name, client port, server host and port, 
        current question, connection status, game over status and last message timestamp.    
        """

        # Client
        self.player_name  = None # Player name
        self.client_port  = self._find_free_port() # Port for incoming messages from the server
        self.has_answered = False # Has the player already answered the current question?
        # Server
        self.server_host = None    # Host of the Quiz Master (leader)    
        self.server_port  = None   # Port of the Quiz Master (leader)
        self.current_question = None # Current question data (dict)
        self.connected    = False # Is the client currently connected to the server?
        self.game_over    = False # Has the game ended?
        self.last_message_time = time.time() # Timestamp of the last message from the server (for reconnect watchdog)

    def _find_free_port(self):
        """
        Search for a free port for the client.
        Input: None
        Calculation: Tries to bind to ports in the range CLIENT_BASE_PORT to CLIENT_BASE_PORT + 100
        Output: Free port number (int)
        Raises: RuntimeError if no free port is found
        """
        for p in range(CLIENT_BASE_PORT, CLIENT_BASE_PORT + 100):
            try:
                test = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                test.bind(("", p))
                test.close()
                return p
            except OSError:
                continue
        raise RuntimeError("Kein freier Port gefunden!")

    def find_quiz_master(self):
        """
        Search for the Quiz Master by querying all ports.
        Input: None
        Calculation: 
        Tries to connect to ports in the range BASE_PORT to BASE_PORT + 20 and sends a "who_is_leader" message. 
        If a response is received with the leader's port, it returns that port.
        Output: Leader port (int) or None if not found        
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

    def send_to_server(self, data):
        """
        Sends a message to the Quiz Master.
        Input: data (dict) - The message to send to the server
        Calculation:
        Tries to connect to the server and send the JSON-encoded data.
        Output: True if the message was sent successfully, False otherwise
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
    # NACHRICHTEN VOM SERVER EMPFANGEN
    # ─────────────────────────────────────────

    def listen_for_messages(self):
        """
        Listens for messages from the server (in a separate thread).
        Input: None
        Calculation:
        Creates a socket, binds it to the client's port, and listens for incoming connections. 
        When a connection is accepted, it receives the data, decodes it from JSON, and passes it to the message handler.
        Output: None
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", self.client_port))
        sock.listen(5)

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
        React on messages from the server.
        Input: msg (dict) - The message received from the server
        Calculation:
        Updates the client's state based on the message type and content.
        Output: None
        """
        self.last_message_time = time.time()
        t = msg.get("type")

        if t == "joined":
            # Successfully joined the game
            self.connected = True
            print(f"\n{msg['message']}")
            print(f"Waiting for more Players...\n")

        elif t == "reconnected":
            # Successfully reconnected to the game after a server failover
            self.connected = True
            print(f"\n{msg['message']}")
            if "current_question" in msg:
                self.current_question = msg["current_question"]
                self.has_answered = False
                self._zeige_question(msg["current_question"])
            else:
                print(f"Waiting for the next question...\n")

        elif t == "join_failed":
            # Joining the game failed (e.g., name already taken)
            print(f"\nJoining failed: {msg['reason']}")
            sys.exit(1)

        elif t == "redirect":
            # Redirected to a new Quiz Master
            self.server_port = msg["leader_port"]
            self.server_host = msg.get("leader_host", self.server_host)
            print(f"\nConnecting to new Quiz Master on port {self.server_port}")

        elif t == "player_joined":
            # A new player has joined the game
            # Difference to "joined": This message is sent to all players when a new player joins, while "joined" is sent only to the player who just joined.
            print(f"👤 '{msg['player_name']}' is joined (in total: {msg['total_players']} Players)")

        elif t == "new_question":
            # A new question has been sent by the server
            self.current_question = msg
            self.has_answered = False
            self._zeige_question(msg)

        elif t == "question_result":
            # The result of the last question has been sent by the server
            self._zeige_ergebnis(msg)

        elif t == "server_failover":
            # The server has failed and a new Quiz Master has been elected
            print(f"\n{msg['message']}")
            if msg.get("leader_port"):
                self.server_port = msg["leader_port"]
                self.server_host = msg.get("leader_host", self.server_host)
                print(f"   Connecting to new Quiz Master on port {self.server_port}...")
            print(f"   Game will continue...\n")

        elif t == "game_over":
            # The game has ended
            self._zeige_endstand(msg)
            self.game_over = True

    def _zeige_question(self, msg):
        """
        Displays a new question.
        Input: msg (dict) - The message containing the question data
        Calculation:
        Displays the question and its details to the user.
        Output: None
        """
        print("\n" + "="*55)
        print(f"  Question {msg['question_number']}/{msg['total_questions']}")
        print("="*55)
        print(f"\n  {msg['question']}\n")
        print(f"  You have {msg['time_limit']} seconds!")
        print(f"\n  Press [w] for TRUE or [f] for FALSE and press Enter")
        print("="*55)

    def _zeige_ergebnis(self, msg):
        """
        Displays the result of a question.
        Input: msg (dict) - The message containing the question data
        Calculation:
        Displays the results and explanation to the user.
        Output: None
        """
        korrekt    = msg["correct_answer"]
        meine_ant  = msg["your_answers"].get(self.player_name)

        print("\n" + "─"*55)
        print(f"  SOLUTION")
        print("─"*55)
        print(f"  Correct Answer: {'TRUE' if korrekt else 'FALSE'}")

        if meine_ant is None:
            print(f"  Your Answer:    Too late / not answered")
        elif meine_ant == korrekt:
            print(f"  Your Answer:    (+1 Point)")
        else:
            print(f"  Your Answer:    Unfortunately wrong")

        print(f"\n  {msg['explanation']}")

        print(f"\nCurrent Leaderboard:")
        # Display the leaderboard with player names and scores, marking the current player with "YOU".
        for i, (name, score) in enumerate(msg["leaderboard"], 1):
            marker = " YOU" if name == self.player_name else ""
            print(f"     {i}. {name}: {score} Points{marker}")
        print("─"*55)

    def _zeige_endstand(self, msg):
        """
        Displays the final standings.
        Input: msg (dict) - The message containing the final score data
        Calculation:
        Displays the final standings and winner announcement.
        Output: None
        """
        print("\n" + "="*55)
        print(f"  !!!! GAME OVER !!!!")
        print("="*55)
        print(f"\n  FINAL SCORE:\n")
        for i, (name, score) in enumerate(msg["leaderboard"], 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "  "
            marker = " YOU" if name == self.player_name else ""
            print(f"     {medal} {i}. {name}: {score} Points{marker}")

        # Winner announcement
        if msg["leaderboard"]:
            sieger, punkte = msg["leaderboard"][0]
            print(f"\n  Winner: {sieger} with {punkte} Points!")

        print("="*55)
        print(f"\n  Press Enter to exit...")

    # ─────────────────────────────────────────
    # RECONNECT-WATCHDOG
    # ─────────────────────────────────────────

    def _watch_for_silence(self):
        """
        Found Server-Crash during silence and tries to reconnect.
        Input: None
        Calculation:
        Monitors the connection for silence and attempts reconnection if necessary.
        Output: None
        """
        while not self.game_over:
            time.sleep(3)
            # Check if the client is connected and the game is not over. 
            if not self.connected or self.game_over:
                continue
            # If the time since the last message exceeds the SILENCE_TIMEOUT, attempt to reconnect.
            if time.time() - self.last_message_time > SILENCE_TIMEOUT:
                print(f"\n No response from server. Searching for a new Quiz Master...")
                self._try_reconnect()

    def _try_reconnect(self):
        """
        Tries to reconnect to the Quiz Master.
        Input: None
        Calculation:
        Attempts to find a new Quiz Master and reconnect.
        Output: None
        """
        new_port = self.find_quiz_master()
        if not new_port:
            print(f"No Quiz Master available. Trying again...")
            self.last_message_time = time.time()  # Reset Timeout, for no Spam
            return
        self.server_port = new_port
        # Send a "join_game" message to the new Quiz Master with the player's details.
        ok = self.send_to_server({
            "type": "join_game",                # Message Type
            "client_host": self.get_local_ip(), # Client Host
            "player_name": self.player_name,    # Player Name
            "client_port": self.client_port     # Client Port
        })
        if ok:
            print(f"Reconnecting to Quiz Master on port {new_port}...")
        else:
            print(f"Reconnection failed.")
        self.last_message_time = time.time()  # Reset not depending of Solution

    # ─────────────────────────────────────────
    # Typing Answer
    # ─────────────────────────────────────────

    def enter_client_solution(self):
        """
        Waiting for player inputs (main thread).
        Input: None
        Calculation:
        Waits for the player to input their answer.
        Output: None
        """
        while not self.game_over:
            try:
                eingabe = input().strip().lower() # strip = remove whitespace, lower = convert to lowercase
            except EOFError:
                break

            if self.game_over: # If the game is over, exit the loop
                break

            # Only react when a question is currently active
            if self.current_question is None or self.has_answered:
                continue

            # Validate the input and convert it to a boolean value
            if eingabe in ["w", "wahr", "true", "t"]: 
                answer = True
            elif eingabe in ["f", "falsch", "false"]:
                answer = False
            else: # Invalid input, prompt the user again
                print(f"  Please enter [w] for TRUE or [f] for FALSE.")
                continue

            # Send the answer to the server
            success = self.send_to_server({
                "type": "submit_answer",        # Message Type 
                "player_name": self.player_name,# Player Name
                "answer": answer               # Answer (True/False)
            })

            if success: # If the answer was sent successfully, mark the question as answered and inform the user
                self.has_answered = True
                print(f"  Your answer '{('TRUE' if answer else 'FALSE')}' has been sent.")
                print(f"     Waiting for other players...")
            else:
                print(f"  Answer could not be sent!")
    
    def get_local_ip(self):
        """
        Returns the local IP address of the client.
        Input: None
        Output: str # Socket IP or Local IP address
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        except:
            return "127.0.0.1"
        finally:
            s.close()

    # ─────────────────────────────────────────
    # START
    # ─────────────────────────────────────────

    def run(self):
        """
        Starts the Quiz Client.
        Input: None
        Calculation:
        Initializes the client, prompts for player name, finds the Quiz Master, and starts listening for messages.
        Output: None
        """
        print("\n" + "="*55)
        print("   DistribuQuiz — Player-Client")
        print("="*55)

        # Enter Player Name
        while True:
            name = input("\nHow do you want to be called? ").strip()
            if name and len(name) <= 20:
                self.player_name = name
                break
            print("   Please enter a valid name (max. 20 characters).")

        # Find Quiz Master
        print("\nSearching for Quiz Master...")
        self.server_port = self.find_quiz_master()

        if not self.server_port:
            print("No Quiz Master found!")
            print("   Make sure at least one server.py is running.")
            sys.exit(1)

        print(f"Quiz Master found on port {self.server_port}")

        # Starting Listener-Thread (for Server-Messages) and Watchdog-Thread (for Reconnect)
        threading.Thread(target=self.listen_for_messages, daemon=True).start()
        threading.Thread(target=self._watch_for_silence, daemon=True).start()
        time.sleep(0.3)

        # Beim Quiz beitreten
        print(f"\nJoining the quiz as '{self.player_name}'...")
        self.send_to_server({
            "type": "join_game",                
            "client_host": self.get_local_ip(),
            "player_name": self.player_name,
            "client_port": self.client_port
        })

        # Waiting for Confirmation
        wartezeit = 0
        while not self.connected and wartezeit < 5:
            time.sleep(0.5)
            wartezeit += 0.5

        if not self.connected:
            print("No response from Quiz Master.")
            sys.exit(1)

        self.last_message_time = time.time()  # Starting Point for Silence-Watchdog

        # Mainthread: Waiting for Player Inputs
        try:
            self.enter_client_solution()
        except KeyboardInterrupt:
            print("\n\nGoodbye!")


if __name__ == "__main__":
    """
    Main entry point for the Quiz Client.
    Initializes and runs the QuizClient.
    """
    QuizClient().run()