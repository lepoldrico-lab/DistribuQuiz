# DistribuQuiz

**DistribuQuiz** is a distributed quiz game system developed as part of a Distributed Systems project (SS26).  
It is inspired by platforms like Kahoot and allows multiple players to participate in real-time quiz sessions while multiple servers coordinate to ensure reliability, fault tolerance, and seamless failover.

---

## Group Information

- **Group ID:** 5  
- **Students:**
  - Lorenz Böckle  
  - Hendrik Stolzke  
  - Rico Lepold

---

## Project Title

**DistribuQuiz – A Distributed Online Quiz Game**

---

## Semester

SS26

---

## Project Overview

This project implements a distributed multiplayer quiz game where multiple clients (players) can join the same quiz session. The system is designed to remain operational even in the presence of server failures.

Key goals:
- Real-time multiplayer quiz gameplay
- Distributed server coordination
- Fault tolerance and failover support
- State synchronization across servers

---

## System Architecture

The system uses a **hybrid architecture** combining:

### 1. Client-Server Model
- Clients connect to a designated **quiz master server**
- The quiz master is responsible for:
  - Distributing questions
  - Collecting answers
  - Calculating scores
  - Broadcasting leaderboard updates
- Communication:
  - Clients → Server: **Unicast (answers, join requests)**
  - Server → Clients: **Multicast (questions, results, updates)**

---

### 2. Peer-to-Peer Server Network
- All servers form a peer network
- Servers continuously exchange:
  - Heartbeat messages
  - Game state updates
- Ensures redundancy and failover capability

---

## Dynamic Discovery

### Server Discovery
- When a new server starts, it broadcasts its presence
- Existing servers respond with:
  - Server ID
  - Network information
- The new server is added to the peer list of all servers

### Client Join Process
- Clients broadcast a join request
- The current quiz master accepts and registers the client
- Other servers ignore the request but receive state updates during synchronization

---

## Fault Tolerance

### Server Failures
- Servers send periodic heartbeat messages
- Failure detection occurs via timeout
- If the **quiz master fails**:
  - A new leader is elected
  - The system restores the latest synchronized game state
  - Game continues with minimal interruption

### Client Failures
- Clients are monitored via response timeouts per question
- If a client fails to respond:
  - It is marked inactive for the round
- Reconnection behavior:
  - Within grace period → state restored
  - Otherwise → permanently removed

---

## Leader Election

The system uses the **Bully Algorithm**:

- Each server has a unique numeric ID
- The highest ID becomes the leader (quiz master)

### Election triggers:
1. System startup  
2. Leader failure detection  
3. A new server joins with a higher ID than current leader  

---

## Leader Responsibilities

The elected quiz master:
- Announces itself to all peers and clients
- Restores the latest synchronized state
- Continues the ongoing quiz session seamlessly

---

## Key Features

- Distributed multiplayer quiz gameplay
- Fault-tolerant server cluster
- Automatic leader election (Bully algorithm)
- Server-to-server state synchronization
- Client recovery support
- Real-time scoring and leaderboard updates

---

## Future Improvements (optional)

- Persistent database for long-term stats
- Web-based UI dashboard for monitoring servers
- Enhanced anti-cheating mechanisms
- Geo-distributed server support

---

## License

Project for academic use (Distributed Systems course, SS26).