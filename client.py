"""
DistribuQuiz - Terminal-Client
===============================
Backup-Variante für Spieler die lieber Terminal nutzen.
(Hauptweg ist das Web-Interface auf http://SERVER-IP:8080)

Starten:  python3 client.py
"""

import socket
import threading
import json
import time
import sys

SERVER_BASE_PORT  = 50100
DISCOVERY_PORT    = 50000
CLIENT_BASE_PORT  = 60000


def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


class QuizClient:
    def __init__(self):
        self.player_name      = None
        self.client_ip        = get_local_ip()
        self.client_port      = self._find_free_port()
        self.server_ip        = None
        self.server_port      = None
        self.current_question = None
        self.has_answered     = False
        self.connected        = False
        self.game_over        = False

    def _find_free_port(self):
        for p in range(CLIENT_BASE_PORT, CLIENT_BASE_PORT + 100):
            try:
                test = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                test.bind(("", p))
                test.close()
                return p
            except OSError:
                continue
        raise RuntimeError("Kein freier Port gefunden!")

    def find_quiz_master_via_udp(self):
        """Sucht den Quiz Master via UDP-Broadcast (für WLAN)."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except (AttributeError, OSError):
            pass
        sock.settimeout(5)

        try:
            sock.bind(("", DISCOVERY_PORT))
        except OSError:
            return None

        print("🔍 Suche Server im WLAN...")
        try:
            data, addr = sock.recvfrom(1024)
            msg = json.loads(data.decode())
            if msg.get("type") == "discovery_announce":
                server_ip   = addr[0]
                server_port = msg["port"]
                # Frag den Server: Wer ist Quiz Master?
                leader_ip, leader_port = self._ask_who_is_leader(server_ip, server_port)
                if leader_ip and leader_port:
                    return leader_ip, leader_port
        except socket.timeout:
            pass
        finally:
            sock.close()
        return None

    def _ask_who_is_leader(self, ip, port):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(2)
                s.connect((ip, port))
                s.sendall(json.dumps({"type": "who_is_leader"}).encode())
                resp = s.recv(1024)
                data = json.loads(resp.decode())
                if data.get("leader_port"):
                    return data.get("leader_ip", ip), data["leader_port"]
        except Exception:
            pass
        return None, None

    def find_quiz_master_locally(self):
        """Sucht lokal (für Tests auf einem Mac)."""
        for port in range(SERVER_BASE_PORT, SERVER_BASE_PORT + 30):
            ip, p = self._ask_who_is_leader("127.0.0.1", port)
            if ip and p:
                return ip, p
        return None, None

    def send_to_server(self, data):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(3)
                s.connect((self.server_ip, self.server_port))
                s.sendall(json.dumps(data).encode())
            return True
        except Exception:
            return False

    def listen_for_messages(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", self.client_port))
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
        t = msg.get("type")
        if t == "joined":
            self.connected = True
            print(f"\n✅ Willkommen, {self.player_name}!")
            print(f"⏳ Warte auf weitere Spieler und Spielstart...\n")
        elif t == "join_failed":
            print(f"\n❌ Beitritt fehlgeschlagen: {msg['reason']}")
            sys.exit(1)

    def poll_state(self):
        """Pollt regelmäßig den Spielzustand vom Server (über die Web-API)."""
        import urllib.request
        last_phase = ""
        last_q_idx = -1
        while not self.game_over:
            try:
                url = f"http://{self.server_ip}:8080/api/state?name={self.player_name}"
                with urllib.request.urlopen(url, timeout=3) as resp:
                    data = json.loads(resp.read().decode())

                phase = data.get("phase", "")
                q_idx = data.get("question_number", -1)

                if phase == "question" and (phase != last_phase or q_idx != last_q_idx):
                    self._zeige_frage(data)
                    last_phase = phase
                    last_q_idx = q_idx
                    self.has_answered = False
                elif phase == "results" and phase != last_phase:
                    self._zeige_ergebnis(data)
                    last_phase = phase
                    self.current_question = None
                elif phase == "finished" and phase != last_phase:
                    self._zeige_endstand(data)
                    self.game_over = True
                    last_phase = phase
                elif phase == "lobby" and phase != last_phase:
                    last_phase = phase
            except Exception:
                pass
            time.sleep(0.8)

    def _zeige_frage(self, data):
        print("\n" + "="*55)
        print(f"  ❓  FRAGE {data.get('question_number', '?')}/{data.get('total_questions', '?')}")
        print("="*55)
        print(f"\n  {data.get('question', '')}\n")
        print(f"  Du hast {data.get('time_left', 10)} Sekunden!")
        print(f"\n  Tippe [w] für WAHR oder [f] für FALSCH und drücke Enter")
        print("="*55)
        self.current_question = data

    def _zeige_ergebnis(self, data):
        korrekt   = data.get("correct_answer")
        meine_ant = data.get("your_answer")
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
        print(f"\n  💡 {data.get('explanation', '')}")
        print(f"\n  🏆 Live-Leaderboard:")
        for i, p in enumerate(data.get("players_list", []), 1):
            marker = " 👈 DU" if p["name"] == self.player_name else ""
            print(f"     {i}. {p['name']}: {p['score']} Punkte{marker}")
        print("─"*55)

    def _zeige_endstand(self, data):
        print("\n" + "="*55)
        print(f"  🏁  SPIEL BEENDET!")
        print("="*55)
        print(f"\n  🏆 ENDSTAND:\n")
        for i, p in enumerate(data.get("players_list", []), 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "  "
            marker = " 👈 DU" if p["name"] == self.player_name else ""
            print(f"     {medal} {i}. {p['name']}: {p['score']} Punkte{marker}")
        if data.get("players_list"):
            sieger = data["players_list"][0]
            print(f"\n  🎉 GEWINNER: {sieger['name']} mit {sieger['score']} Punkten!")
        print("="*55)
        print(f"\n  Drücke Enter zum Beenden...")

    def antwort_eingeben(self):
        while not self.game_over:
            try:
                eingabe = input().strip().lower()
            except EOFError:
                break
            if self.game_over:
                break
            if self.current_question is None or self.has_answered:
                continue

            if eingabe in ["w", "wahr", "true", "t"]:
                antwort = True
            elif eingabe in ["f", "falsch", "false"]:
                antwort = False
            else:
                print(f"  ⚠️  Bitte [w] oder [f] eingeben.")
                continue

            erfolg = self.send_to_server({
                "type": "submit_answer",
                "player_name": self.player_name,
                "answer": antwort
            })
            if erfolg:
                self.has_answered = True
                print(f"  ✅ '{('WAHR' if antwort else 'FALSCH')}' gesendet. Warte auf andere...")
            else:
                print(f"  ❌ Antwort konnte nicht gesendet werden!")

    def run(self):
        print("\n" + "="*55)
        print("  🎮  DistribuQuiz — Terminal-Client")
        print("="*55)
        print(f"\n  Tipp: Du kannst auch im Browser spielen!")
        print(f"        http://<server-ip>:8080\n")

        # Server suchen
        print("🔍 Suche Quiz Master...")
        # Erst lokal probieren
        ip, port = self.find_quiz_master_locally()
        if not ip:
            # Dann im WLAN
            result = self.find_quiz_master_via_udp()
            if result:
                ip, port = result

        if not ip:
            print("❌ Kein Quiz Master gefunden!")
            print("   Manuelle Eingabe:")
            ip = input("   Server-IP (z.B. 192.168.178.42): ").strip()
            port_str = input("   Server-Port (z.B. 50100): ").strip()
            try:
                port = int(port_str)
            except ValueError:
                print("Ungültiger Port.")
                sys.exit(1)
            # Frag wer Quiz Master ist
            real_ip, real_port = self._ask_who_is_leader(ip, port)
            if real_ip and real_port:
                ip, port = real_ip, real_port

        self.server_ip   = ip
        self.server_port = port
        print(f"✅ Quiz Master gefunden: {ip}:{port}")

        # Name eingeben
        while True:
            name = input("\nWie heißt du? ").strip()
            if name and len(name) <= 20:
                self.player_name = name
                break
            print("  ⚠️  Bitte gültigen Namen eingeben (max. 20 Zeichen).")

        # Listener starten
        threading.Thread(target=self.listen_for_messages, daemon=True).start()
        time.sleep(0.3)

        # Beitreten
        print(f"\n📤 Trete bei als '{self.player_name}'...")
        self.send_to_server({
            "type": "join_game",
            "player_name": self.player_name,
            "client_ip": self.client_ip,
            "client_port": self.client_port
        })

        # Auf Bestätigung warten
        wait = 0
        while not self.connected and wait < 5:
            time.sleep(0.5)
            wait += 0.5
        if not self.connected:
            print("❌ Keine Antwort vom Server.")
            sys.exit(1)

        # State-Polling starten
        threading.Thread(target=self.poll_state, daemon=True).start()

        try:
            self.antwort_eingeben()
        except KeyboardInterrupt:
            print("\n\n👋 Auf Wiedersehen!")


if __name__ == "__main__":
    QuizClient().run()
