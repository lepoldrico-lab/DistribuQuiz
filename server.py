"""
DistribuQuiz - Server (Web + Terminal)
=======================================
Verteiltes Quiz-System mit:
- 3 Servern (Discovery, Voting, Heartbeat, Fault Tolerance)
- Game-PIN System (Kahoot-Style)
- Web-Interface (Browser-Spieler)
- Terminal-Client (Backup-Variante)
- WLAN-Support (mehrere Laptops)

Starten:  python3 server.py
"""

import socket
import threading
import json
import time
import random
import os
import http.server
import socketserver
from urllib.parse import urlparse, parse_qs

# ─────────────────────────────────────────────
# KONFIGURATION
# ─────────────────────────────────────────────

SERVER_BASE_PORT = 50100      # Server-zu-Server + Terminal-Clients
WEB_BASE_PORT    = 8080       # Web-Interface
DISCOVERY_PORT   = 50000      # UDP-Broadcast für WLAN-Discovery

HEARTBEAT_SEC    = 2
TIMEOUT_SEC      = 15
QUESTION_SEC     = 10
PAUSE_SEC        = 4
MIN_PLAYERS      = 1
QUESTIONS_FILE   = "questions.json"


# ─────────────────────────────────────────────
# HILFSFUNKTIONEN
# ─────────────────────────────────────────────

def send_msg(host, port, data: dict):
    """Schickt eine JSON-Nachricht via TCP."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(3)
            s.connect((host, port))
            s.sendall(json.dumps(data).encode())
        return True
    except Exception:
        return False


def get_local_ip():
    """Findet die eigene IP im WLAN."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def generate_pin():
    """Erstellt einen 4-stelligen Game-PIN."""
    return f"QUIZ{random.randint(10, 99)}"


# ─────────────────────────────────────────────
# WEB-SERVER (HTML für Browser-Spieler)
# ─────────────────────────────────────────────

class WebHandler(http.server.SimpleHTTPRequestHandler):
    """Handler für HTTP-Anfragen vom Browser."""

    quiz_server = None  # Wird von außen gesetzt

    def log_message(self, format, *args):
        pass  # Keine HTTP-Logs im Terminal

    def do_GET(self):
        path = urlparse(self.path).path

        if path == "/" or path == "/index.html":
            self._send_html(HTML_PAGE)

        elif path == "/api/state":
            # Spieler fragt: Was ist der aktuelle Spielzustand?
            params = parse_qs(urlparse(self.path).query)
            player_name = params.get("name", [""])[0]
            self._send_json(self.quiz_server.get_state_for_player(player_name))

        elif path == "/api/info":
            # Info über das Spiel (PIN, Anzahl Server)
            self._send_json({
                "pin": self.quiz_server.game_pin,
                "is_quiz_master": self.quiz_server._is_quiz_master(),
                "server_id": self.quiz_server.server_id,
                "num_peers": len(self.quiz_server.peers) + 1
            })

        else:
            self.send_error(404)

    def do_POST(self):
        path = urlparse(self.path).path
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode()

        try:
            data = json.loads(body) if body else {}
        except Exception:
            data = {}

        if path == "/api/join":
            # Spieler will beitreten
            pin    = data.get("pin", "").upper().strip()
            name   = data.get("name", "").strip()

            if pin != self.quiz_server.game_pin:
                self._send_json({"success": False, "error": "Falscher Game-PIN"})
                return
            if not name or len(name) > 20:
                self._send_json({"success": False, "error": "Ungültiger Name (1-20 Zeichen)"})
                return

            ok, msg = self.quiz_server.web_join(name)
            self._send_json({"success": ok, "error": msg if not ok else None})

        elif path == "/api/answer":
            # Spieler sendet Antwort
            name   = data.get("name", "").strip()
            answer = data.get("answer", None)
            if name and isinstance(answer, bool):
                self.quiz_server.web_submit_answer(name, answer)
            self._send_json({"success": True})

        else:
            self.send_error(404)

    def _send_html(self, content):
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(content.encode("utf-8"))

    def _send_json(self, obj):
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(obj).encode("utf-8"))


# ─────────────────────────────────────────────
# HTML-SEITE (Kahoot-Style mit Farben!)
# ─────────────────────────────────────────────

HTML_PAGE = """<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>DistribuQuiz</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  min-height: 100vh;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 20px;
  color: #333;
}
.container {
  background: white;
  border-radius: 20px;
  padding: 40px;
  max-width: 600px;
  width: 100%;
  box-shadow: 0 20px 60px rgba(0,0,0,0.3);
}
h1 { font-size: 2.5em; text-align: center; margin-bottom: 10px;
  background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
  -webkit-background-clip: text; -webkit-text-fill-color: transparent;
}
.subtitle { text-align: center; color: #666; margin-bottom: 30px; font-size: 1.1em; }
input, button {
  width: 100%; padding: 15px; font-size: 1.2em;
  border-radius: 12px; border: 2px solid #e0e0e0;
  margin-bottom: 15px; font-family: inherit;
}
input:focus { outline: none; border-color: #667eea; }
button {
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  color: white; border: none; cursor: pointer; font-weight: 600;
  transition: transform 0.1s;
}
button:hover { transform: translateY(-2px); }
button:active { transform: translateY(0); }
button:disabled { opacity: 0.5; cursor: not-allowed; transform: none; }

.btn-true { background: linear-gradient(135deg, #43e97b 0%, #38f9d7 100%); }
.btn-false { background: linear-gradient(135deg, #fa709a 0%, #fee140 100%); }
.btn-row { display: grid; grid-template-columns: 1fr 1fr; gap: 15px; }
.btn-row button { font-size: 2em; padding: 30px; margin: 0; }

.question-box {
  background: linear-gradient(135deg, #ffecd2 0%, #fcb69f 100%);
  padding: 30px; border-radius: 15px; margin-bottom: 20px;
  font-size: 1.5em; text-align: center; font-weight: 600;
}
.timer {
  font-size: 1.3em; text-align: center; padding: 15px;
  background: #fff3cd; border-radius: 10px; margin-bottom: 20px;
  color: #856404;
}
.result {
  padding: 25px; border-radius: 15px; margin-bottom: 20px;
  text-align: center; font-size: 1.2em;
}
.result.correct { background: linear-gradient(135deg, #84fab0 0%, #8fd3f4 100%); }
.result.wrong { background: linear-gradient(135deg, #ff9a9e 0%, #fecfef 100%); }
.explanation { margin-top: 15px; font-size: 0.95em; opacity: 0.9; }

.leaderboard { background: #f8f9fa; border-radius: 15px; padding: 20px; }
.leaderboard h3 { margin-bottom: 15px; color: #495057; }
.player-row {
  display: flex; justify-content: space-between; align-items: center;
  padding: 12px 15px; background: white; border-radius: 10px;
  margin-bottom: 8px;
}
.player-row.me { background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%); font-weight: 700; }
.player-row .rank { font-size: 1.4em; margin-right: 10px; }
.player-row .name { flex-grow: 1; }
.player-row .score { font-weight: 700; color: #667eea; }

.lobby { text-align: center; }
.lobby .pin-display {
  font-size: 2em; font-weight: 700;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  color: white; padding: 20px; border-radius: 15px; margin: 20px 0;
  letter-spacing: 5px;
}
.lobby .waiting { font-size: 1.2em; color: #666; margin: 20px 0; }
.lobby .player-count { font-size: 1.5em; color: #667eea; font-weight: 700; }

.gameover { text-align: center; }
.gameover .winner {
  font-size: 1.5em; padding: 20px;
  background: linear-gradient(135deg, #f6d365 0%, #fda085 100%);
  border-radius: 15px; margin-bottom: 20px;
}
.error { color: #dc3545; padding: 10px; background: #f8d7da; border-radius: 8px; margin-bottom: 15px; }
.info-bar {
  display: flex; justify-content: space-between; padding: 10px 15px;
  background: #f0f0f0; border-radius: 10px; margin-bottom: 20px;
  font-size: 0.9em; color: #666;
}
.failover-banner {
  background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
  color: white; padding: 15px; border-radius: 10px; margin-bottom: 20px;
  text-align: center; font-weight: 600;
}
</style>
</head>
<body>
<div class="container" id="app"></div>

<script>
let playerName = '';
let lastPhase = '';
let answered = false;
let pollInterval = null;

const app = document.getElementById('app');

function render(html) {
  app.innerHTML = html;
}

function showJoin(error) {
  render(`
    <h1>🎮 DistribuQuiz</h1>
    <p class="subtitle">Verteiltes Quiz-System</p>
    ${error ? `<div class="error">${error}</div>` : ''}
    <input type="text" id="pin" placeholder="Game-PIN (z.B. QUIZ42)" maxlength="6">
    <input type="text" id="name" placeholder="Dein Name" maxlength="20">
    <button onclick="join()">Beitreten 🚀</button>
  `);
}

async function join() {
  const pin = document.getElementById('pin').value.trim().toUpperCase();
  const name = document.getElementById('name').value.trim();
  if (!pin || !name) {
    showJoin('Bitte PIN und Name eingeben.');
    return;
  }
  try {
    const res = await fetch('/api/join', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ pin, name })
    });
    const data = await res.json();
    if (data.success) {
      playerName = name;
      startPolling();
    } else {
      showJoin(data.error || 'Beitritt fehlgeschlagen.');
    }
  } catch (e) {
    showJoin('Verbindung zum Server fehlgeschlagen.');
  }
}

function startPolling() {
  if (pollInterval) clearInterval(pollInterval);
  poll();
  pollInterval = setInterval(poll, 800);
}

async function poll() {
  try {
    const res = await fetch('/api/state?name=' + encodeURIComponent(playerName));
    const data = await res.json();
    updateUI(data);
  } catch (e) {
    // ignore
  }
}

function updateUI(data) {
  if (data.phase !== lastPhase) {
    answered = false;
    lastPhase = data.phase;
  }

  if (data.phase === 'lobby') {
    render(`
      <div class="lobby">
        <h1>🎮 Lobby</h1>
        <div class="pin-display">${data.pin}</div>
        <p class="waiting">Warte auf weitere Spieler...</p>
        <div class="player-count">${data.num_players} Spieler bereit</div>
        ${renderPlayers(data.players_list, true)}
      </div>
    `);
  } else if (data.phase === 'question') {
    const banner = data.failover ? `<div class="failover-banner">🛡️ Server-Wechsel! Spiel läuft weiter</div>` : '';
    render(`
      ${banner}
      <h1>Frage ${data.question_number}/${data.total_questions}</h1>
      <div class="timer">⏱️ ${data.time_left} Sekunden</div>
      <div class="question-box">${escapeHtml(data.question)}</div>
      ${answered
        ? `<div class="result"><strong>✅ Antwort gesendet!</strong><br>Warte auf andere Spieler...</div>`
        : `<div class="btn-row">
            <button class="btn-true" onclick="answer(true)">WAHR ✅</button>
            <button class="btn-false" onclick="answer(false)">FALSCH ❌</button>
          </div>`
      }
      ${renderPlayers(data.players_list, false)}
    `);
  } else if (data.phase === 'results') {
    const myAnswer = data.your_answer;
    const correct = data.correct_answer;
    const isCorrect = myAnswer === correct;
    const noAnswer = myAnswer === null || myAnswer === undefined;

    let resultBox;
    if (noAnswer) {
      resultBox = `<div class="result wrong">
        <strong>⏰ Zu spät!</strong><br>
        Richtige Antwort war: <strong>${correct ? 'WAHR ✅' : 'FALSCH ❌'}</strong>
        <div class="explanation">💡 ${escapeHtml(data.explanation || '')}</div>
      </div>`;
    } else if (isCorrect) {
      resultBox = `<div class="result correct">
        <strong>🎉 Richtig! +1 Punkt</strong>
        <div class="explanation">💡 ${escapeHtml(data.explanation || '')}</div>
      </div>`;
    } else {
      resultBox = `<div class="result wrong">
        <strong>❌ Leider falsch</strong><br>
        Richtige Antwort: <strong>${correct ? 'WAHR ✅' : 'FALSCH ❌'}</strong>
        <div class="explanation">💡 ${escapeHtml(data.explanation || '')}</div>
      </div>`;
    }

    render(`
      <h1>Auflösung</h1>
      ${resultBox}
      ${renderPlayers(data.players_list, false)}
    `);
  } else if (data.phase === 'finished') {
    const winner = data.players_list[0];
    render(`
      <div class="gameover">
        <h1>🏁 Spiel beendet!</h1>
        <div class="winner">🏆 Gewinner: <strong>${escapeHtml(winner.name)}</strong> mit ${winner.score} Punkten!</div>
        ${renderPlayers(data.players_list, false)}
      </div>
    `);
  }
}

function renderPlayers(players, isLobby) {
  if (!players || players.length === 0) return '';
  let html = '<div class="leaderboard"><h3>' + (isLobby ? '👥 Spieler in der Lobby' : '🏆 Live-Leaderboard') + '</h3>';
  players.forEach((p, i) => {
    const isMe = p.name === playerName;
    const medal = i === 0 ? '🥇' : i === 1 ? '🥈' : i === 2 ? '🥉' : `${i+1}.`;
    html += `<div class="player-row${isMe ? ' me' : ''}">
      <span class="rank">${medal}</span>
      <span class="name">${escapeHtml(p.name)}${isMe ? ' (du)' : ''}</span>
      <span class="score">${p.score} Pkt</span>
    </div>`;
  });
  html += '</div>';
  return html;
}

async function answer(value) {
  answered = true;
  poll();
  try {
    await fetch('/api/answer', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: playerName, answer: value })
    });
  } catch (e) {}
}

function escapeHtml(s) {
  if (s === null || s === undefined) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

showJoin();
</script>
</body>
</html>
"""


# ─────────────────────────────────────────────
# HAUPT-KLASSE: QuizServer
# ─────────────────────────────────────────────

class QuizServer:
    def __init__(self):
        self.server_id   = random.randint(1000, 9999)
        self.local_ip    = get_local_ip()
        self.port        = self._find_free_port(SERVER_BASE_PORT)
        self.web_port    = self._find_free_port(WEB_BASE_PORT)

        self.leader_id   = None
        self.leader_ip   = None
        self.leader_port = None

        self.peers       = {}      # {server_id: {ip, port, last_seen}}
        self.lock        = threading.Lock()

        # Game-PIN (wird vom Quiz Master später gesetzt)
        self.game_pin    = generate_pin()

        # Quiz-Spielzustand (Backups erhalten Kopien davon!)
        self.game_state = {
            "phase": "lobby",
            "players": {},                    # {name: {score, port (optional), is_web}}
            "current_question_idx": -1,
            "current_question": None,
            "answers_this_round": {},
            "question_start_time": 0,
            "game_pin": self.game_pin,
            "last_result": None,              # für Auflösungs-Phase
            "failover_flag": False
        }

        self.questions = self._load_questions()

        print(f"\n{'='*60}")
        print(f"  🎮  DistribuQuiz Server gestartet!")
        print(f"{'='*60}")
        print(f"  Server-ID  : {self.server_id}")
        print(f"  WLAN-IP    : {self.local_ip}")
        print(f"  Server-Port: {self.port}")
        print(f"  Web-Port   : {self.web_port}")
        print(f"  Fragen     : {len(self.questions)} geladen")
        print(f"{'='*60}\n")

    def _find_free_port(self, base):
        for p in range(base, base + 30):
            try:
                test = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                test.bind(("", p))
                test.close()
                return p
            except OSError:
                continue
        raise RuntimeError(f"Kein freier Port ab {base} gefunden!")

    def _load_questions(self):
        if not os.path.exists(QUESTIONS_FILE):
            print(f"❌ FEHLER: {QUESTIONS_FILE} nicht gefunden!")
            return []
        with open(QUESTIONS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)

    # ─────────────────────────────────────────
    # DYNAMIC DISCOVERY (UDP-Broadcast für WLAN!)
    # ─────────────────────────────────────────

    def udp_broadcast_listener(self):
        """Hört auf UDP-Broadcasts anderer Server im WLAN."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except (AttributeError, OSError):
            pass
        sock.bind(("", DISCOVERY_PORT))

        while True:
            try:
                data, addr = sock.recvfrom(1024)
                msg = json.loads(data.decode())
                if msg.get("type") != "discovery_announce":
                    continue
                if msg.get("server_id") == self.server_id:
                    continue  # ignoriere eigenen Broadcast

                sender_ip   = addr[0]
                sender_port = msg["port"]
                sender_id   = msg["server_id"]

                with self.lock:
                    is_new = sender_id not in self.peers
                    self.peers[sender_id] = {
                        "ip": sender_ip,
                        "port": sender_port,
                        "last_seen": time.time()
                    }

                if is_new:
                    print(f"[Discovery] ➕ Neuer Server entdeckt: ID={sender_id} @ {sender_ip}:{sender_port}")
                    # TCP-"Hello" zurücksenden mit unserer Info
                    send_msg(sender_ip, sender_port, {
                        "type": "hello",
                        "server_id": self.server_id,
                        "ip": self.local_ip,
                        "port": self.port,
                        "leader_id": self.leader_id,
                        "leader_ip": self.leader_ip,
                        "leader_port": self.leader_port
                    })
            except Exception:
                continue

    def udp_broadcast_sender(self):
        """Sendet alle 3 Sek einen UDP-Broadcast 'Hier bin ich!' ins Netzwerk."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        msg = {
            "type": "discovery_announce",
            "server_id": self.server_id,
            "port": self.port,
            "web_port": self.web_port
        }

        # Auch lokal nochmal probieren (für Solo-Modus auf einem Mac)
        while True:
            try:
                payload = json.dumps(msg).encode()
                sock.sendto(payload, ("<broadcast>", DISCOVERY_PORT))
                sock.sendto(payload, ("127.0.0.1", DISCOVERY_PORT))
            except Exception:
                pass
            time.sleep(3)

    # ─────────────────────────────────────────
    # VOTING: Bully-Algorithmus
    # ─────────────────────────────────────────

    def start_election(self):
        """Wahl: Server mit höchster ID wird Quiz Master."""
        print(f"\n[Wahl] 🗳️  Wahl gestartet! Meine ID: {self.server_id}")

        with self.lock:
            all_servers = {self.server_id: (self.local_ip, self.port)}
            for sid, info in self.peers.items():
                all_servers[sid] = (info["ip"], info["port"])

        winner_id = max(all_servers.keys())
        winner_ip, winner_port = all_servers[winner_id]

        was_leader_before = (self.leader_id == self.server_id)
        self.leader_id   = winner_id
        self.leader_ip   = winner_ip
        self.leader_port = winner_port

        if winner_id == self.server_id:
            print(f"[Wahl] 👑 ICH bin der neue Quiz Master! (ID: {self.server_id})")
            self._print_pin_banner()
            if not was_leader_before:
                if self.game_state["phase"] in ["question", "results"]:
                    print(f"[Wahl] 🛡️  Übernehme laufendes Spiel von ausgefallenem Server!")
                    self.game_state["failover_flag"] = True
                    threading.Thread(target=self._continue_game, daemon=True).start()
        else:
            print(f"[Wahl] Quiz Master gewählt: Server {winner_id} @ {winner_ip}:{winner_port}")

        # Alle anderen informieren
        with self.lock:
            for sid, info in self.peers.items():
                send_msg(info["ip"], info["port"], {
                    "type": "new_leader",
                    "leader_id": winner_id,
                    "leader_ip": winner_ip,
                    "leader_port": winner_port,
                    "game_pin": self.game_pin
                })

    def _print_pin_banner(self):
        """Zeigt den Game-PIN groß im Terminal an."""
        print(f"\n")
        print(f"  ╔══════════════════════════════════════╗")
        print(f"  ║                                      ║")
        print(f"  ║   🎮  GAME-PIN: {self.game_pin:<10}      ║")
        print(f"  ║                                      ║")
        print(f"  ║   Browser öffnen:                    ║")
        print(f"  ║   http://{self.local_ip}:{self.web_port:<5}        ║")
        print(f"  ║                                      ║")
        print(f"  ╚══════════════════════════════════════╝")
        print(f"\n")

    # ─────────────────────────────────────────
    # FAULT TOLERANCE: Heartbeat
    # ─────────────────────────────────────────

    def send_heartbeats(self):
        while True:
            time.sleep(HEARTBEAT_SEC)
            with self.lock:
                peers_copy = dict(self.peers)
            for sid, info in peers_copy.items():
                send_msg(info["ip"], info["port"], {
                    "type": "heartbeat",
                    "server_id": self.server_id,
                    "ip": self.local_ip,
                    "port": self.port
                })

    def check_for_failures(self):
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
                if sid == self.leader_id:
                    print(f"[Fault Tolerance] 🚨 Quiz Master ausgefallen! Starte Neuwahl...")
                    self.leader_id = None
                    threading.Thread(target=self.start_election, daemon=True).start()

    # ─────────────────────────────────────────
    # QUIZ-LOGIK
    # ─────────────────────────────────────────

    def _is_quiz_master(self):
        return self.leader_id == self.server_id

    def _sync_to_backups(self):
        """Synchronisiert Spielzustand mit Backup-Servern."""
        with self.lock:
            state_copy = json.loads(json.dumps(self.game_state))
            peers_copy = dict(self.peers)
        for sid, info in peers_copy.items():
            send_msg(info["ip"], info["port"], {
                "type": "state_sync",
                "game_state": state_copy,
                "game_pin": self.game_pin
            })

    def run_quiz(self):
        """Wartet auf Spieler und startet das Spiel."""
        while True:
            time.sleep(2)
            if not self._is_quiz_master():
                continue
            if self.game_state["phase"] != "lobby":
                continue

            with self.lock:
                num_players = len(self.game_state["players"])

            if num_players >= MIN_PLAYERS:
                # Kurz warten ob noch mehr Spieler kommen
                time.sleep(8)
                with self.lock:
                    num_now = len(self.game_state["players"])
                if num_now >= MIN_PLAYERS and self.game_state["phase"] == "lobby":
                    print(f"\n[Quiz] 🎯 {num_now} Spieler bereit. Spiel beginnt!")
                    self._start_game()

    def _start_game(self):
        random.shuffle(self.questions)
        self._play_questions(start_idx=0)

    def _continue_game(self):
        idx = self.game_state["current_question_idx"]
        if idx < 0 or idx >= len(self.questions):
            return
        print(f"[Quiz] 🛡️  Setze Spiel fort ab Frage {idx+1}")
        time.sleep(2)
        self._play_questions(start_idx=idx)

    def _play_questions(self, start_idx):
        for idx in range(start_idx, len(self.questions)):
            if not self._is_quiz_master():
                return

            frage = self.questions[idx]
            with self.lock:
                self.game_state["phase"] = "question"
                self.game_state["current_question_idx"] = idx
                self.game_state["current_question"] = frage
                self.game_state["answers_this_round"] = {}
                self.game_state["question_start_time"] = time.time()
                self.game_state["last_result"] = None

            print(f"\n[Quiz] ❓ Frage {idx+1}/{len(self.questions)}: {frage['frage']}")
            self._sync_to_backups()

            # Warten bis Zeit abgelaufen ODER alle geantwortet
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

            with self.lock:
                self.game_state["failover_flag"] = False

        self._finish_game()

    def _auswerten(self, frage):
        with self.lock:
            self.game_state["phase"] = "results"
            antworten = dict(self.game_state["answers_this_round"])
            korrekt = frage["antwort"]

            for player_name, antwort in antworten.items():
                if antwort == korrekt:
                    if player_name in self.game_state["players"]:
                        self.game_state["players"][player_name]["score"] += 1

            self.game_state["last_result"] = {
                "correct_answer": korrekt,
                "explanation": frage["erklaerung"],
                "answers": antworten
            }

            rangliste = sorted(
                [(name, info["score"]) for name, info in self.game_state["players"].items()],
                key=lambda x: x[1],
                reverse=True
            )

        print(f"[Quiz] 📊 Richtige Antwort: {'WAHR' if korrekt else 'FALSCH'}")
        for i, (name, score) in enumerate(rangliste, 1):
            print(f"        {i}. {name}: {score} Punkte")

        self._sync_to_backups()

    def _finish_game(self):
        with self.lock:
            self.game_state["phase"] = "finished"

        rangliste = sorted(
            [(name, info["score"]) for name, info in self.game_state["players"].items()],
            key=lambda x: x[1],
            reverse=True
        )
        print(f"\n[Quiz] 🏆 Spiel beendet!")
        for i, (name, score) in enumerate(rangliste, 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "  "
            print(f"        {medal} {i}. {name}: {score} Punkte")

        self._sync_to_backups()

    # ─────────────────────────────────────────
    # WEB-API (für Browser-Spieler)
    # ─────────────────────────────────────────

    def web_join(self, player_name):
        """Spieler tritt über Web bei."""
        if not self._is_quiz_master():
            return False, "Dieser Server ist kein Quiz Master. Bitte verbinde dich mit dem Quiz Master."

        with self.lock:
            if player_name in self.game_state["players"]:
                return False, "Name bereits vergeben."
            if self.game_state["phase"] != "lobby":
                return False, "Spiel läuft bereits — bitte warten bis es vorbei ist."

            self.game_state["players"][player_name] = {
                "score": 0,
                "is_web": True
            }
            num = len(self.game_state["players"])

        print(f"[Quiz] 👤 Web-Spieler beigetreten: '{player_name}' (insgesamt: {num})")
        self._sync_to_backups()
        return True, None

    def web_submit_answer(self, player_name, answer):
        if not self._is_quiz_master():
            return
        with self.lock:
            if self.game_state["phase"] != "question":
                return
            if player_name not in self.game_state["players"]:
                return
            self.game_state["answers_this_round"][player_name] = answer
        print(f"[Quiz] 📝 {player_name}: {'WAHR' if answer else 'FALSCH'}")

    def get_state_for_player(self, player_name):
        """Gibt Spielzustand zurück (was der Spieler sehen soll)."""
        with self.lock:
            phase = self.game_state["phase"]
            players_list = sorted(
                [{"name": n, "score": info["score"]}
                 for n, info in self.game_state["players"].items()],
                key=lambda x: x["score"],
                reverse=True
            )

            result = {
                "phase": phase,
                "pin": self.game_pin,
                "num_players": len(self.game_state["players"]),
                "players_list": players_list,
                "failover": self.game_state.get("failover_flag", False)
            }

            if phase == "question":
                q = self.game_state["current_question"]
                if q:
                    elapsed = time.time() - self.game_state["question_start_time"]
                    time_left = max(0, int(QUESTION_SEC - elapsed))
                    result.update({
                        "question": q["frage"],
                        "question_number": self.game_state["current_question_idx"] + 1,
                        "total_questions": len(self.questions),
                        "time_left": time_left
                    })
                    # Hat dieser Spieler schon geantwortet?
                    if player_name in self.game_state["answers_this_round"]:
                        result["already_answered"] = True

            elif phase == "results":
                last = self.game_state.get("last_result")
                if last:
                    result.update({
                        "correct_answer": last["correct_answer"],
                        "explanation": last["explanation"],
                        "your_answer": last["answers"].get(player_name)
                    })

        return result

    # ─────────────────────────────────────────
    # NACHRICHTEN EMPFANGEN (TCP)
    # ─────────────────────────────────────────

    def handle_message(self, conn, addr):
        try:
            conn.settimeout(3)
            data = b""
            while True:
                chunk = conn.recv(16384)
                if not chunk:
                    break
                data += chunk
                if len(data) > 1_000_000:
                    break
                # Versuchen zu parsen, sonst weiter empfangen
                try:
                    json.loads(data.decode())
                    break
                except Exception:
                    continue
            if not data:
                return
            msg = json.loads(data.decode())

            if msg.get("type") == "who_is_leader":
                conn.sendall(json.dumps({
                    "leader_id": self.leader_id,
                    "leader_ip": self.leader_ip,
                    "leader_port": self.leader_port
                }).encode())
                return

            self._process(msg, addr)
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _process(self, msg, addr):
        t = msg.get("type")
        sender_ip = addr[0] if addr else "127.0.0.1"

        if t == "hello":
            sid = msg["server_id"]
            ip  = msg.get("ip", sender_ip)
            with self.lock:
                self.peers[sid] = {
                    "ip": ip,
                    "port": msg["port"],
                    "last_seen": time.time()
                }
            print(f"[Discovery] 👋 Hello von Server {sid} @ {ip}:{msg['port']}")

            # Wenn der andere schon einen Leader kennt → übernehmen
            if msg.get("leader_id") and self.leader_id is None:
                self.leader_id   = msg["leader_id"]
                self.leader_ip   = msg.get("leader_ip")
                self.leader_port = msg["leader_port"]
                print(f"[Discovery] Bekannter Quiz Master übernommen: Server {self.leader_id}")

            # Antwort zurücksenden
            send_msg(ip, msg["port"], {
                "type": "welcome",
                "server_id": self.server_id,
                "ip": self.local_ip,
                "port": self.port,
                "leader_id": self.leader_id,
                "leader_ip": self.leader_ip,
                "leader_port": self.leader_port,
                "game_pin": self.game_pin if self._is_quiz_master() else None
            })

            # Wahl auslösen
            threading.Thread(target=self.start_election, daemon=True).start()

        elif t == "welcome":
            sid = msg["server_id"]
            ip  = msg.get("ip", sender_ip)
            with self.lock:
                self.peers[sid] = {
                    "ip": ip,
                    "port": msg["port"],
                    "last_seen": time.time()
                }
            if msg.get("leader_id"):
                self.leader_id   = msg["leader_id"]
                self.leader_ip   = msg.get("leader_ip")
                self.leader_port = msg["leader_port"]
                if msg.get("game_pin"):
                    self.game_pin = msg["game_pin"]
                    self.game_state["game_pin"] = self.game_pin
                print(f"[Discovery] Quiz Master: Server {self.leader_id}, PIN: {self.game_pin}")
            threading.Thread(target=self.start_election, daemon=True).start()

        elif t == "heartbeat":
            sid = msg["server_id"]
            with self.lock:
                if sid in self.peers:
                    self.peers[sid]["last_seen"] = time.time()
                else:
                    self.peers[sid] = {
                        "ip": msg.get("ip", sender_ip),
                        "port": msg["port"],
                        "last_seen": time.time()
                    }

        elif t == "new_leader":
            self.leader_id   = msg["leader_id"]
            self.leader_ip   = msg.get("leader_ip")
            self.leader_port = msg["leader_port"]
            if msg.get("game_pin"):
                self.game_pin = msg["game_pin"]
                self.game_state["game_pin"] = self.game_pin
            if self.leader_id == self.server_id:
                print(f"[Wahl] 👑 Ich bin der neue Quiz Master!")
                self._print_pin_banner()
            else:
                print(f"[Wahl] Neuer Quiz Master: Server {self.leader_id}")

        elif t == "state_sync":
            with self.lock:
                self.game_state = msg["game_state"]
                if msg.get("game_pin"):
                    self.game_pin = msg["game_pin"]
            phase = self.game_state.get("phase", "?")
            num_players = len(self.game_state.get("players", {}))
            print(f"[Sync] 💾 State aktualisiert (Phase: {phase}, Spieler: {num_players})")

        # Terminal-Client Nachrichten (Backup-Variante)
        elif t == "join_game":
            self._handle_terminal_join(msg)
        elif t == "submit_answer":
            self._handle_terminal_answer(msg)

    def _handle_terminal_join(self, msg):
        """Terminal-Client tritt bei."""
        if not self._is_quiz_master():
            return
        player_name = msg["player_name"]
        client_ip   = msg.get("client_ip", "127.0.0.1")
        client_port = msg["client_port"]

        with self.lock:
            if player_name in self.game_state["players"]:
                send_msg(client_ip, client_port, {
                    "type": "join_failed",
                    "reason": "Name bereits vergeben."
                })
                return
            self.game_state["players"][player_name] = {
                "score": 0,
                "ip": client_ip,
                "port": client_port,
                "is_web": False
            }
            num = len(self.game_state["players"])

        print(f"[Quiz] 👤 Terminal-Spieler: '{player_name}' (insgesamt: {num})")
        send_msg(client_ip, client_port, {
            "type": "joined",
            "player_name": player_name
        })
        self._sync_to_backups()

    def _handle_terminal_answer(self, msg):
        if not self._is_quiz_master():
            return
        player_name = msg["player_name"]
        answer = msg["answer"]
        with self.lock:
            if self.game_state["phase"] != "question":
                return
            if player_name not in self.game_state["players"]:
                return
            self.game_state["answers_this_round"][player_name] = answer
        print(f"[Quiz] 📝 {player_name}: {'WAHR' if answer else 'FALSCH'}")

    def listen_for_connections(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", self.port))
        sock.listen(20)
        while True:
            conn, addr = sock.accept()
            threading.Thread(target=self.handle_message, args=(conn, addr), daemon=True).start()

    def start_web_server(self):
        """Startet den HTTP-Server für Browser-Spieler."""
        WebHandler.quiz_server = self
        socketserver.TCPServer.allow_reuse_address = True
        with socketserver.ThreadingTCPServer(("", self.web_port), WebHandler) as httpd:
            httpd.serve_forever()

    # ─────────────────────────────────────────
    # START
    # ─────────────────────────────────────────

    def run(self):
        threads = [
            threading.Thread(target=self.listen_for_connections, daemon=True),
            threading.Thread(target=self.udp_broadcast_listener,  daemon=True),
            threading.Thread(target=self.udp_broadcast_sender,    daemon=True),
            threading.Thread(target=self.send_heartbeats,         daemon=True),
            threading.Thread(target=self.check_for_failures,      daemon=True),
            threading.Thread(target=self.run_quiz,                daemon=True),
            threading.Thread(target=self.start_web_server,        daemon=True),
        ]
        for t in threads:
            t.start()

        # Kurz warten dass andere Server uns finden
        time.sleep(5)

        if self.leader_id is None:
            print("[Wahl] Kein Quiz Master gefunden → starte Wahl...")
            self.start_election()

        print(f"\n[Server] ✅ Läuft. Drücke Ctrl+C zum Beenden.\n")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[Server] Beende...")


if __name__ == "__main__":
    QuizServer().run()
