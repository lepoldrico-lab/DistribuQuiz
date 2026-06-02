"""
DistribuQuiz - Client (Spieler)
================================
Verbindet sich mit dem Quiz Master und nimmt am Quiz teil.

Starten:  python3 client.py
"""

import socket
import threading
import json
import time
import sys

# ─────────────────────────────────────────────
# KONFIGURATION
# ─────────────────────────────────────────────

BASE_PORT        = 50100      # Server laufen auf Ports 50100, 50101, ...
CLIENT_BASE_PORT = 60000      # Clients hören auf Ports 60000, 60001, ...


# ─────────────────────────────────────────────
# CLIENT-KLASSE
# ─────────────────────────────────────────────

SILENCE_TIMEOUT = 25   # Sekunden ohne Server-Nachricht → Reconnect

class QuizClient:
    def __init__(self):
        self.player_name  = None
        self.server_host = None
        self.client_port  = self._find_free_port()
        self.server_port  = None       # Port des aktuellen Quiz Masters
        self.current_question = None
        self.has_answered = False
        self.connected    = False
        self.game_over    = False
        self.last_message_time = time.time()

    def _find_free_port(self):
        """Sucht einen freien Port für den Client."""
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
        """Sucht den Quiz Master indem alle Ports abgefragt werden."""
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
        """Schickt eine Nachricht an den Quiz Master."""
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
        """Hört auf Nachrichten vom Server (in eigenem Thread)."""
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
        """Reagiert auf Nachrichten vom Server."""
        self.last_message_time = time.time()
        t = msg.get("type")

        if t == "joined":
            self.connected = True
            print(f"\n✅ {msg['message']}")
            print(f"⏳ Warte auf weitere Spieler...\n")

        elif t == "reconnected":
            self.connected = True
            print(f"\n🔄 {msg['message']}")
            if "current_question" in msg:
                self.current_question = msg["current_question"]
                self.has_answered = False
                self._zeige_frage(msg["current_question"])
            else:
                print(f"⏳ Warte auf die nächste Frage...\n")

        elif t == "join_failed":
            print(f"\n❌ Beitritt fehlgeschlagen: {msg['reason']}")
            sys.exit(1)

        elif t == "redirect":
            # Quiz Master hat sich geändert
            self.server_port = msg["leader_port"]
            self.server_host = msg.get("leader_host", self.server_host)
            print(f"\n🔄 Verbinde neu mit Quiz Master auf Port {self.server_port}")

        elif t == "player_joined":
            print(f"👤 '{msg['player_name']}' ist beigetreten (insgesamt: {msg['total_players']} Spieler)")

        elif t == "new_question":
            self.current_question = msg
            self.has_answered = False
            self._zeige_frage(msg)

        elif t == "question_result":
            self._zeige_ergebnis(msg)

        elif t == "server_failover":
            print(f"\n🛡️  {msg['message']}")
            if msg.get("leader_port"):
                self.server_port = msg["leader_port"]
                self.server_host = msg.get("leader_host", self.server_host)
                print(f"   Verbinde mit neuem Quiz Master auf Port {self.server_port}...")
            print(f"   Spiel wird fortgesetzt...\n")

        elif t == "game_over":
            self._zeige_endstand(msg)
            self.game_over = True

    def _zeige_frage(self, msg):
        """Zeigt eine neue Frage an."""
        print("\n" + "="*55)
        print(f"  ❓  FRAGE {msg['question_number']}/{msg['total_questions']}")
        print("="*55)
        print(f"\n  {msg['question']}\n")
        print(f"  Du hast {msg['time_limit']} Sekunden!")
        print(f"\n  Tippe [w] für WAHR oder [f] für FALSCH und drücke Enter")
        print("="*55)

    def _zeige_ergebnis(self, msg):
        """Zeigt das Ergebnis einer Frage an."""
        korrekt    = msg["correct_answer"]
        meine_ant  = msg["your_answers"].get(self.player_name)

        print("\n" + "─"*55)
        print(f"  📊  AUFLÖSUNG")
        print("─"*55)
        print(f"  Richtige Antwort: {'✅ WAHR' if korrekt else '❌ FALSCH'}")

        if meine_ant is None:
            print(f"  Deine Antwort:    ⏰ Zu spät / nicht geantwortet")
        elif meine_ant == korrekt:
            print(f"  Deine Antwort:    ✅ Richtig! (+1 Punkt)")
        else:
            print(f"  Deine Antwort:    ❌ Leider falsch")

        print(f"\n  💡 {msg['explanation']}")

        print(f"\n  🏆 Aktuelle Rangliste:")
        for i, (name, score) in enumerate(msg["leaderboard"], 1):
            marker = " 👈 DU" if name == self.player_name else ""
            print(f"     {i}. {name}: {score} Punkte{marker}")
        print("─"*55)

    def _zeige_endstand(self, msg):
        """Zeigt das Endergebnis an."""
        print("\n" + "="*55)
        print(f"  🏁  SPIEL BEENDET!")
        print("="*55)
        print(f"\n  🏆 ENDSTAND:\n")
        for i, (name, score) in enumerate(msg["leaderboard"], 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "  "
            marker = " 👈 DU" if name == self.player_name else ""
            print(f"     {medal} {i}. {name}: {score} Punkte{marker}")

        # Gewinner
        if msg["leaderboard"]:
            sieger, punkte = msg["leaderboard"][0]
            print(f"\n  🎉 GEWINNER: {sieger} mit {punkte} Punkten!")

        print("="*55)
        print(f"\n  Drücke Enter zum Beenden...")

    # ─────────────────────────────────────────
    # RECONNECT-WATCHDOG
    # ─────────────────────────────────────────

    def _watch_for_silence(self):
        """Erkennt Server-Ausfall anhand von Stille und versucht Wiederverbindung."""
        while not self.game_over:
            time.sleep(3)
            if not self.connected or self.game_over:
                continue
            if time.time() - self.last_message_time > SILENCE_TIMEOUT:
                print(f"\n⚠️  Keine Antwort vom Server. Suche neuen Quiz Master...")
                self._try_reconnect()

    def _try_reconnect(self):
        new_port = self.find_quiz_master()
        if not new_port:
            print(f"❌ Kein Quiz Master erreichbar. Versuche erneut...")
            self.last_message_time = time.time()  # Reset, damit kein Spam
            return
        self.server_port = new_port
        ok = self.send_to_server({
            "type": "join_game",
            "client_host": self.get_local_ip(),
            "player_name": self.player_name,
            "client_port": self.client_port
        })
        if ok:
            print(f"🔄 Wiederverbindung zu Quiz Master auf Port {new_port} gesendet...")
        else:
            print(f"❌ Wiederverbindung fehlgeschlagen.")
        self.last_message_time = time.time()  # Reset unabhängig vom Ergebnis

    # ─────────────────────────────────────────
    # ANTWORT EINGEBEN
    # ─────────────────────────────────────────

    def antwort_eingeben(self):
        """Wartet auf Spieler-Eingaben (Hauptthread)."""
        while not self.game_over:
            try:
                eingabe = input().strip().lower()
            except EOFError:
                break

            if self.game_over:
                break

            # Nur reagieren wenn gerade eine Frage aktiv ist
            if self.current_question is None or self.has_answered:
                continue

            if eingabe in ["w", "wahr", "true", "t"]:
                antwort = True
            elif eingabe in ["f", "falsch", "false"]:
                antwort = False
            else:
                print(f"  ⚠️  Bitte [w] für WAHR oder [f] für FALSCH eingeben.")
                continue

            erfolg = self.send_to_server({
                "type": "submit_answer",
                "player_name": self.player_name,
                "answer": antwort
            })

            if erfolg:
                self.has_answered = True
                print(f"  ✅ Deine Antwort '{('WAHR' if antwort else 'FALSCH')}' wurde gesendet.")
                print(f"     Warte auf andere Spieler...")
            else:
                print(f"  ❌ Antwort konnte nicht gesendet werden!")
    
    def get_local_ip(self):
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
        print("\n" + "="*55)
        print("  🎮  DistribuQuiz — Spieler-Client")
        print("="*55)

        # Spielername eingeben
        while True:
            name = input("\nWie heißt du? ").strip()
            if name and len(name) <= 20:
                self.player_name = name
                break
            print("  ⚠️  Bitte einen gültigen Namen eingeben (max. 20 Zeichen).")

        # Quiz Master finden
        print("\n🔍 Suche Quiz Master...")
        self.server_port = self.find_quiz_master()

        if not self.server_port:
            print("❌ Kein Quiz Master gefunden!")
            print("   Stelle sicher, dass mindestens ein server.py läuft.")
            sys.exit(1)

        print(f"✅ Quiz Master gefunden auf Port {self.server_port}")

        # Listener-Thread starten (für Server-Nachrichten)
        threading.Thread(target=self.listen_for_messages, daemon=True).start()
        threading.Thread(target=self._watch_for_silence, daemon=True).start()
        time.sleep(0.3)

        # Beim Quiz beitreten
        print(f"\n📤 Trete dem Quiz bei als '{self.player_name}'...")
        self.send_to_server({
            "type": "join_game",
            "client_host": self.get_local_ip(),
            "player_name": self.player_name,
            "client_port": self.client_port
        })

        # Auf Bestätigung warten
        wartezeit = 0
        while not self.connected and wartezeit < 5:
            time.sleep(0.5)
            wartezeit += 0.5

        if not self.connected:
            print("❌ Keine Antwort vom Quiz Master.")
            sys.exit(1)

        self.last_message_time = time.time()  # Startpunkt für Silence-Watchdog

        # Hauptthread: Auf Spieler-Eingaben warten
        try:
            self.antwort_eingeben()
        except KeyboardInterrupt:
            print("\n\n👋 Auf Wiedersehen!")


if __name__ == "__main__":
    QuizClient().run()
