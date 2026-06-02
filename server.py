"""
DistribuQuiz - Server
======================
Verteiltes Quiz-System: Mehrere Server arbeiten zusammen.
Einer ist Quiz Master (Leader), die anderen sind Backups.
Wenn der Quiz Master ausfällt, übernimmt automatisch ein Backup.

Starten:  python3 server.py
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

DISCOVERY_PORT  = 55000
BASE_PORT       = 50100       # Server laufen auf Ports 50100, 50101, ...
HEARTBEAT_SEC   = 2           # Alle 2 Sek: "Ich lebe noch!"
TIMEOUT_SEC     = 15          # Nach 15 Sek ohne Signal: Server ausgefallen
QUESTION_SEC    = 10          # Wie lange Spieler pro Frage Zeit haben
PAUSE_SEC       = 3           # Pause zwischen den Fragen (Auflösung zeigen)
MIN_PLAYERS     = 1           # Mindestanzahl Spieler um zu starten
QUESTIONS_FILE  = "questions.json"


# ─────────────────────────────────────────────
# HILFSFUNKTION: Nachrichten senden
# ─────────────────────────────────────────────

def send_msg(host, port, data: dict):
    """Schickt eine JSON-Nachricht an einen anderen Server oder Client."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(3)
            s.connect((host, port))
            s.sendall(json.dumps(data).encode())
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────
# HAUPT-KLASSE: Server
# ─────────────────────────────────────────────

class QuizServer:
    def __init__(self):
        self.server_id = str(uuid.uuid4())
        self.port        = self._find_free_port()

        self.leader_id   = None
        self.leader_port = None

        self.peers       = {}      # {server_id: {port, last_seen}}
        self.lock        = threading.Lock()

        # Quiz-Spielzustand (Backups erhalten Kopien davon!)
        self.game_state = {
            "phase": "lobby",          # lobby | question | results | finished
            "players": {},             # {player_name: {port, score}}
            "current_question_idx": -1,
            "current_question": None,
            "answers_this_round": {},
            "question_start_time": 0
        }

        self.questions = self._load_questions()

        print(f"\n{'='*55}")
        print(f"  🎮  DistribuQuiz Server gestartet!")
        print(f"  Server-ID : {self.server_id}")
        print(f"  Port      : {self.port}")
        print(f"  Fragen    : {len(self.questions)} geladen")
        print(f"{'='*55}\n")

    def _find_free_port(self):
        """Sucht einen freien Port ab BASE_PORT."""
        for p in range(BASE_PORT, BASE_PORT + 20):
            try:
                test = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                test.bind(("", p))
                test.close()
                return p
            except OSError:
                continue
        raise RuntimeError("Kein freier Port gefunden!")

    def _load_questions(self):
        """Lädt die Fragen aus der JSON-Datei."""
        if not os.path.exists(QUESTIONS_FILE):
            print(f"❌ FEHLER: {QUESTIONS_FILE} nicht gefunden!")
            return []
        with open(QUESTIONS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)

    # ─────────────────────────────────────────
    # DYNAMIC DISCOVERY
    # ─────────────────────────────────────────

    def broadcast_presence(self):
        print("[Discovery] Suche Server im Netzwerk...")

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.settimeout(2)

        discovery_msg = {
            "type": "discover_server"
        }

        sock.sendto(
            json.dumps(discovery_msg).encode(),
            ("255.255.255.255", DISCOVERY_PORT)
        )

        found = 0

        while True:
            try:
                data, addr = sock.recvfrom(4096)
                msg = json.loads(data.decode())

                sid = msg["server_id"]

                if sid == self.server_id:
                    continue

                with self.lock:
                    self.peers[sid] = {
                        "port": msg["port"],
                        "host": addr[0],
                        "last_seen": time.time()
                    }

                found += 1

                print(
                    f"[Discovery] Server gefunden: "
                    f"ID={sid} "
                    f"IP={addr[0]} "
                    f"Port={msg['port']}"
                )

            except socket.timeout:
                break

        print(f"[Discovery] {found} Server gefunden")
    # ─────────────────────────────────────────
    # VOTING: Bully-Algorithmus
    # ─────────────────────────────────────────

    def start_election(self):
        """Wahl: Server mit höchster ID wird Quiz Master."""
        print(f"\n[Wahl] 🗳️  Wahl gestartet! Meine ID: {self.server_id}")

        with self.lock:
            all_ids = {self.server_id: self.port}
            for sid, info in self.peers.items():
                all_ids[sid] = info["port"]

        winner_id   = max(all_ids.keys())
        winner_port = all_ids[winner_id]

        was_leader_before = (self.leader_id == self.server_id)
        self.leader_id   = winner_id
        self.leader_port = winner_port

        if winner_id == self.server_id:
            print(f"[Wahl] 👑 ICH bin der neue Quiz Master! (ID: {self.server_id})")
            if not was_leader_before:
                # Frisch gewählt - falls ein Spiel lief, übernimm es
                if self.game_state["phase"] in ["question", "results"]:
                    print(f"[Wahl] 🛡️  Übernehme laufendes Spiel von ausgefallenem Server!")
                    threading.Thread(target=self._continue_game, daemon=True).start()
        else:
            print(f"[Wahl] Quiz Master gewählt: Server {winner_id} (Port {winner_port})")

        with self.lock:
            peers_copy = dict(self.peers)
        for sid, info in peers_copy.items():
            send_msg(info["host"], info["port"], {
                "type": "new_leader",
                "leader_id": winner_id,
                "leader_port": winner_port
            })

    # ─────────────────────────────────────────
    # FAULT TOLERANCE: Heartbeat
    # ─────────────────────────────────────────

    def send_heartbeats(self):
        """Sendet alle 2 Sekunden 'Ich lebe noch!' an alle Peers."""
        while True:
            time.sleep(HEARTBEAT_SEC)
            with self.lock:
                peers_copy = dict(self.peers)
            for sid, info in peers_copy.items():
                send_msg(info["host"], info["port"], {
                    "type": "heartbeat",
                    "server_id": self.server_id,
                    "port": self.port
                })

    def check_for_failures(self):
        """Prüft regelmäßig ob Server ausgefallen sind."""
        while True:
            time.sleep(HEARTBEAT_SEC)
            now = time.time()
            failed = []

            with self.lock:
                for sid, info in self.peers.items():
                    if now - info["last_seen"] > TIMEOUT_SEC:
                        failed.append(sid)

            for sid in failed:
                with self.lock:
                    self.peers.pop(sid, None)
                print(f"\n[Fault Tolerance] ⚠️  Server {sid} ausgefallen!")

                # Wenn der Quiz Master ausgefallen ist → Neuwahl!
                if sid == self.leader_id:
                    print(f"[Fault Tolerance] 🚨 Quiz Master ausgefallen! Starte Neuwahl...")
                    self.leader_id = None
                    threading.Thread(target=self.start_election, daemon=True).start()

    # ─────────────────────────────────────────
    # QUIZ-LOGIK (nur Quiz Master macht das aktiv)
    # ─────────────────────────────────────────

    def _is_quiz_master(self):
        return self.leader_id == self.server_id

    def _broadcast_to_players(self, message):
        """Sendet eine Nachricht an alle verbundenen Spieler."""
        with self.lock:
            players_copy = dict(self.game_state["players"])
        for name, info in players_copy.items():
            send_msg(info["host"], info["port"], message)

    def _sync_to_backups(self):
        """Sendet den aktuellen Spielzustand an alle Backup-Server."""
        with self.lock:
            state_copy = json.loads(json.dumps(self.game_state))
            peers_copy = dict(self.peers)
        for sid, info in peers_copy.items():
            send_msg(info["host"], info["port"], {
                "type": "state_sync",
                "game_state": state_copy
            })

    def run_quiz(self):
        """Startet das Spiel sobald genug Spieler in der Lobby sind."""
        while True:
            time.sleep(2)
            if not self._is_quiz_master():
                continue
            if self.game_state["phase"] != "lobby":
                continue

            with self.lock:
                num_players = len(self.game_state["players"])

            if num_players >= MIN_PLAYERS:
                print(f"\n[Quiz] 🎯 {num_players} Spieler bereit. Starte das Spiel!")
                self._start_game()

    def _start_game(self):
        """Stellt nacheinander alle Fragen."""
        random.shuffle(self.questions)
        self._play_questions(start_idx=0)

    def _get_own_host(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        except Exception:
            return "127.0.0.1"
        finally:
            s.close()

    def _continue_game(self):
        """Wird von einem neu gewählten Quiz Master aufgerufen."""
        idx = self.game_state["current_question_idx"]
        if idx < 0 or idx >= len(self.questions):
            return

        print(f"[Quiz] 🛡️  Setze Spiel fort ab Frage {idx+1}")

        self._broadcast_to_players({
            "type": "server_failover",
            "message": "Quiz Master ist ausgefallen — neuer Quiz Master übernimmt!",
            "leader_port": self.port,
            "leader_host": self._get_own_host()
        })

        time.sleep(2)
        self._play_questions(start_idx=idx)

    def _play_questions(self, start_idx):
        """Spielt alle Fragen ab start_idx."""
        for idx in range(start_idx, len(self.questions)):
            if not self._is_quiz_master():
                print(f"[Quiz] Bin nicht mehr Quiz Master — beende Spielleitung.")
                return

            frage = self.questions[idx]

            with self.lock:
                self.game_state["phase"] = "question"
                self.game_state["current_question_idx"] = idx
                self.game_state["current_question"] = frage
                self.game_state["answers_this_round"] = {}
                self.game_state["question_start_time"] = time.time()

            print(f"\n[Quiz] ❓ Frage {idx+1}/{len(self.questions)}: {frage['frage']}")

            self._broadcast_to_players({
                "type": "new_question",
                "question_number": idx + 1,
                "total_questions": len(self.questions),
                "question": frage["frage"],
                "time_limit": QUESTION_SEC
            })
            self._sync_to_backups()

            # Warten bis Zeit abgelaufen ODER alle geantwortet haben
            start = time.time()
            while time.time() - start < QUESTION_SEC:
                with self.lock:
                    num_players  = len(self.game_state["players"])
                    num_answered = len(self.game_state["answers_this_round"])
                if num_players > 0 and num_answered >= num_players:
                    print(f"[Quiz] ✅ Alle haben geantwortet!")
                    break
                time.sleep(0.3)

            self._auswerten(frage)

            if not self._is_quiz_master():
                return

            time.sleep(PAUSE_SEC)

        self._finish_game()

    def _auswerten(self, frage):
        """Wertet die Antworten aus und vergibt Punkte."""
        with self.lock:
            self.game_state["phase"] = "results"
            antworten = dict(self.game_state["answers_this_round"])
            korrekt = frage["antwort"]

            for uid, antwort in antworten.items():
                if uid in self.game_state["players"]:
                    if antwort == korrekt:
                        self.game_state["players"][uid]["score"] += 1

            antworten_by_name = {
                self.game_state["players"][uid]["name"]: antwort
                for uid, antwort in antworten.items()
                if uid in self.game_state["players"]
            }

            rangliste = sorted(
                [
                    (info["name"], info["score"])
                    for info in self.game_state["players"].values()
                ],
                key=lambda x: x[1],
                reverse=True
            )

        print(f"[Quiz] 📊 Richtige Antwort: {'WAHR' if korrekt else 'FALSCH'}")
        print(f"[Quiz] 📊 Aktuelle Rangliste:")
        for i, (name, score) in enumerate(rangliste, 1):
            print(f"        {i}. {name}: {score} Punkte")

        self._broadcast_to_players({
            "type": "question_result",
            "correct_answer": korrekt,
            "explanation": frage["erklaerung"],
            "your_answers": antworten_by_name,
            "leaderboard": rangliste
        })
        self._sync_to_backups()

    def _finish_game(self):
        """Beendet das Spiel und kürt den Gewinner."""
        with self.lock:
            self.game_state["phase"] = "finished"
            rangliste = sorted(
                [(info["name"], info["score"]) for info in self.game_state["players"].values()],
                key=lambda x: x[1],
                reverse=True
            )

        print(f"\n[Quiz] 🏆 Spiel beendet!")
        print(f"[Quiz] 🏆 Endstand:")
        for i, (name, score) in enumerate(rangliste, 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "  "
            print(f"        {medal} {i}. {name}: {score} Punkte")

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
                    "phase": "lobby",
                    "players": {},
                    "current_question_idx": -1,
                    "current_question": None,
                    "answers_this_round": {},
                    "question_start_time": 0
                }
                print(f"[Quiz] 🔄 Spiel zurückgesetzt — warte auf neue Spieler...")
                do_sync = True
        if do_sync:
            self._sync_to_backups()

    # ─────────────────────────────────────────
    # NACHRICHTEN EMPFANGEN
    # ─────────────────────────────────────────

    def handle_message(self, conn, addr):
        try:
            conn.settimeout(3)
            data = conn.recv(16384)
            if not data:
                return
            msg = json.loads(data.decode())

            if msg.get("type") == "who_is_leader":
                conn.sendall(json.dumps({
                    "leader_id": self.leader_id,
                    "leader_port": self.leader_port,
                    "leader_host": self.peers[self.leader_id]["host"]
                }).encode())
                return

            self._process(msg, addr[0])
        except Exception:
            pass
        finally:
            conn.close()

    def _process(self, msg, peer_host="127.0.0.1"):
        t = msg.get("type")

        # ─── Server ↔ Server ───
        if t == "hello":
            with self.lock:
                self.peers[msg["server_id"]] = {
                    "port": msg["port"],
                    "host": peer_host,
                    "last_seen": time.time()
                }
            print(f"[Discovery] ➕ Neuer Server: ID={msg['server_id']} Port={msg['port']}")
            send_msg(peer_host, msg["port"], {
                "type": "welcome",
                "server_id": self.server_id,
                "port": self.port,
                "leader_id": self.leader_id,
                "leader_port": self.leader_port
            })

        elif t == "welcome":
            with self.lock:
                self.peers[msg["server_id"]] = {
                    "port": msg["port"],
                    "host": peer_host,
                    "last_seen": time.time()
                }
            if msg.get("leader_id"):
                self.leader_id   = msg["leader_id"]
                self.leader_port = msg["leader_port"]
                print(f"[Discovery] Bekannter Quiz Master: Server {self.leader_id}")
            threading.Thread(target=self.start_election, daemon=True).start()

        elif t == "heartbeat":
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
            self.leader_id   = msg["leader_id"]
            self.leader_port = msg["leader_port"]
            if self.leader_id == self.server_id:
                print(f"[Wahl] 👑 Ich bin der neue Quiz Master!")
            else:
                print(f"[Wahl] Neuer Quiz Master: Server {self.leader_id}")

        elif t == "state_sync":
            # Backup übernimmt Spielstand vom Quiz Master
            with self.lock:
                self.game_state = msg["game_state"]
            phase = self.game_state.get("phase", "?")
            num_players = len(self.game_state.get("players", {}))
            print(f"[Sync] 💾 Spielzustand aktualisiert (Phase: {phase}, Spieler: {num_players})")

        # ─── Client ↔ Server ───
        elif t == "join_game":
            if not self._is_quiz_master():
                send_msg("127.0.0.1", msg["client_port"], {
                    "type": "redirect",
                    "leader_port": self.leader_port
                })
                return

            player_name = msg["player_name"]
            client_port = msg["client_port"]
            client_host = msg.get("client_host", "127.0.0.1")

            reject = False
            reconnect_data = None
            num_players = 0

            with self.lock:
                if self.game_state["phase"] == "finished":
                    self.game_state = {
                        "phase": "lobby",
                        "players": {},
                        "current_question_idx": -1,
                        "current_question": None,
                        "answers_this_round": {},
                        "question_start_time": 0
                    }
                    print(f"[Quiz] 🔄 Neues Spiel gestartet (Spieler hat sich verbunden)")

                existing_uid = None
                for uid, p in self.game_state["players"].items():
                    if p["name"] == player_name:
                        existing_uid = uid
                        break

                if existing_uid is not None:
                    if self.game_state["phase"] == "lobby":
                        reject = True
                    else:
                        # Reconnect during running game: update address, restore score
                        self.game_state["players"][existing_uid]["port"] = client_port
                        self.game_state["players"][existing_uid]["host"] = client_host
                        score = self.game_state["players"][existing_uid]["score"]
                        q_data = None
                        if self.game_state["phase"] == "question" and self.game_state["current_question"]:
                            elapsed = time.time() - self.game_state["question_start_time"]
                            remaining = max(1, int(QUESTION_SEC - elapsed))
                            q_data = {
                                "type": "new_question",
                                "question_number": self.game_state["current_question_idx"] + 1,
                                "total_questions": len(self.questions),
                                "question": self.game_state["current_question"]["frage"],
                                "time_limit": remaining
                            }
                        reconnect_data = {"score": score, "q_data": q_data}
                else:
                    player_uid = str(uuid.uuid4())
                    self.game_state["players"][player_uid] = {
                        "name": player_name,
                        "uid": player_uid,
                        "port": client_port,
                        "host": client_host,
                        "score": 0
                    }
                    num_players = len(self.game_state["players"])

            if reject:
                send_msg(client_host, client_port, {
                    "type": "join_failed",
                    "reason": "Name bereits vergeben."
                })
                return

            if reconnect_data is not None:
                payload = {
                    "type": "reconnected",
                    "player_name": player_name,
                    "score": reconnect_data["score"],
                    "message": f"Willkommen zurück, {player_name}! Dein Punktestand: {reconnect_data['score']} Punkt(e)."
                }
                if reconnect_data["q_data"]:
                    payload["current_question"] = reconnect_data["q_data"]
                send_msg(client_host, client_port, payload)
                self._sync_to_backups()
                print(f"[Quiz] 🔄 Spieler '{player_name}' wieder verbunden (Score: {reconnect_data['score']})")
                return

            print(f"[Quiz] 👤 Neuer Spieler: '{player_name}' (insgesamt: {num_players})")

            send_msg(
                client_host,
                client_port,
                {
                    "type": "joined",
                    "player_name": player_name,
                    "message": f"Willkommen {player_name}!"
                }
            )

            self._broadcast_to_players({
                "type": "player_joined",
                "player_name": player_name,
                "total_players": num_players
            })

            self._sync_to_backups()

        elif t == "submit_answer":
            if not self._is_quiz_master():
                return

            player_name = msg["player_name"]
            answer = msg["answer"]

            with self.lock:
                if self.game_state["phase"] != "question":
                    return
                player_uid = None
                for uid, p in self.game_state["players"].items():
                    if p["name"] == player_name:
                        player_uid = uid
                        break
                if player_uid is None:
                    return

                self.game_state["answers_this_round"][player_uid] = answer

            print(f"[Quiz] 📝 {player_name} hat geantwortet: {'WAHR' if answer else 'FALSCH'}")

    def discovery_listener(self):
        """Lauscht auf UDP-Broadcasts und antwortet mit eigenen Server-Infos."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind(("0.0.0.0", DISCOVERY_PORT))
        while True:
            try:
                data, addr = sock.recvfrom(4096)
                msg = json.loads(data.decode())
                if msg.get("type") == "discover_server":
                    response = {
                        "type": "server_present",
                        "server_id": self.server_id,
                        "port": self.port
                    }
                    sock.sendto(json.dumps(response).encode(), addr)
            except Exception:
                pass

    def discovery_loop(self):
        """Wiederholt regelmäßig die Discovery, um neue Server zu finden."""
        while True:
            time.sleep(30)
            self.broadcast_presence()

    def listen_for_connections(self):
        """Hört auf eingehende TCP-Verbindungen."""
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind(("0.0.0.0", self.port))
        server_sock.listen(20)
        while True:
            conn, addr = server_sock.accept()
            threading.Thread(target=self.handle_message, args=(conn, addr), daemon=True).start()

    # ─────────────────────────────────────────
    # START
    # ─────────────────────────────────────────

    def run(self):
        threads = [
            threading.Thread(target=self.listen_for_connections, daemon=True),
            threading.Thread(target=self.send_heartbeats,        daemon=True),
            threading.Thread(target=self.check_for_failures,     daemon=True),
            threading.Thread(target=self.run_quiz,               daemon=True),
            threading.Thread(target=self.discovery_listener, daemon=True),
            threading.Thread(target=self.discovery_loop, daemon=True),
        ]
        for t in threads:
            t.start()

        time.sleep(0.5)
        self.broadcast_presence()

        time.sleep(4)
        if self.leader_id is None:
            print("[Wahl] Kein Quiz Master gefunden → starte Wahl...")
            self.start_election()

        print("\n[Server] ✅ Läuft. Drücke Ctrl+C zum Beenden.\n")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[Server] Beende...")


if __name__ == "__main__":
    QuizServer().run()
