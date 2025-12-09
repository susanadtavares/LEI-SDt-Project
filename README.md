# LEI-SDt-Project

Repository for the Final Project of the Distributed Systems Curricular Unit

## 📩 Distributed System for Document Storage with FAISS and IPFS
This project implements a fault-tolerant distributed system for decentralized document storage and semantic search, combining IPFS, RAFT and FAISS into a single cohesive platform. The goal is to allow multiple peers to collaborate in a network where documents are uploaded, democratically approved by the community, and queried through an advanced semantic search interface.​

#### System overview
The system uses IPFS for immutable and decentralized file storage, RAFT for deterministic leader election and coordination, and FAISS with sentence embeddings for efficient real-time semantic search over distributed documents. Peers communicate through PubSub, forming a resilient network that can tolerate node failures while maintaining a single logical leader for orchestration.​

#### Core architecture
Each node maintains a RAFT state context with role (Follower, Candidate, Leader), current term, votedFor, leaderId, and timestamps for heartbeat monitoring, all protected with thread-safe structures for concurrent access. The architecture includes class and sequence diagrams (hosted externally) that describe interactions between RAFT components, the HTTP API, IPFS, FAISS, and background services like garbage collection.​

#### RAFT and fault tolerance
Leader election is managed through an election loop that sends and processes RequestVote messages, counts majorities, and promotes nodes to leader when required. Periodic leader and peer heartbeats, combined with timeouts, detect failures and trigger new elections, ensuring continuous availability and consistent coordination in the cluster.​

#### HTTP API and document workflow
When a node becomes leader, it starts a FastAPI HTTP server exposing endpoints for document upload, semantic search, status, document listing, and IPFS-based download. Document uploads go through a voting phase and a two-phase commit protocol that distributes confirmed versions across peers, ensuring consistent state and replicated document indexes.​

#### Semantic search and maintenance
Semantic search requests create search sessions that are dispatched to peers in a round-robin fashion or processed locally, using FAISS indexes and transformer-based embeddings to retrieve the most relevant documents. A background garbage collector periodically cleans old voting sessions, expired confirmations, inactive peers, and stale search data, while a diagnostic script validates environment readiness (Python, modules, IPFS, PubSub, network) before starting a node.

💻 Tech Stack
Python

👥 Authors
David Borges

José Arrais

Patrícia Oliveira

Susana Tavares

⚠️ Disclaimer
This is a university project and may contain suboptimal practices or even errors.

If you have any questions or suggestions, feel free to ask.

This repository is public and it is intended for archival and educational purposes only.

## Installation

1. Create and activate the virtual environment:
```bash
python -m venv venv
venv\Scripts\activate # Windows
source venv/bin/activate # Linux/MacOS
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Start the IPFS daemon:
```bash
ipfs daemon --enable-pubsub-experiment
```

## Installation Verification

1. Check if all dependencies are installed:
```bash
python ipfs/check_setup.py
```

## Testing Procedure

1. Start the system:
```bash
python ipfs/node.py
```

2. Start the client:
```bash
python ipfs/test_upload.py
```

## Documentation

The API documentation is available at:
http://localhost:5000/docs
