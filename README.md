# Uber-Style Infrastructure Blueprint Clone

A production-ready infrastructure blueprint designed for high-throughput, low-latency ride-hailing and localized delivery systems. This project serves as a concrete implementation of a **System Design Architecture** blueprint, decoupling user interface concerns to focus strictly on foundational infrastructure challenges: real-time geospatial processing, asynchronous event streaming, and persistent analytical auditing.

## 🏗️ Architecture Overview

The system architecture leverages a reactive, decoupled design pattern across three core layers:

[ Driver Telemetry Pings ]
│
▼
┌──────────────────────────────────────┐
│  DriverTrackingService (Python)      │
└────┬────────────────────────────┬────┘
│                            │
│ (Fast GIS Lookups)         │ (Publish Events)
▼                            ▼
┌──────────────────┐        ┌────────────────────────────────┐
│   Redis Cache    │        │  Apache Kafka Event Broker     │
│  (Geospatial)    │        └───────────────┬────────────────┘
└──────────────────┘                        │
│ (Async Consumption)
▼
┌────────────────────────────────┐
│ HistoryIngestionService (Py)   │
└───────────────┬────────────────┘
│
│ (Bulk Persistence)
▼
┌────────────────────────────────┐
│    PostgreSQL Database         │
└────────────────────────────────┘

1. **In-Memory Geospatial Cache (Redis):** Handles ultra-low latency write and read operations for continuous driver coordinate updates. Utilizing Redis GIS commands (`GEOADD`, `GEORADIUS`), the matching engine can query nearby available drivers within milliseconds.
2. **Distributed Event Streaming (Apache Kafka):** Acts as the asynchronous backbone. Driver telemetry coordinates are ingested into a highly scalable pipeline (`driver-telemetry` topic) running on an agile, modern KRaft (Kafka Raft) metadata mode—eliminating any external ZooKeeper overhead.
3. **Analytical Historical Storage (PostgreSQL):** A persistent relational layer optimized via composite database indices to ingest, partition, and safely store immutable historical lifecycle data for long-term reporting and financial ledger tracking.

---

## 🛠️ Tech Stack & Components

* **Python 3.10+** (Core Microservices Application)
* **Apache Kafka (Confluent Platform 7.4.0)** (Event Streaming in KRaft Mode)
* **Redis 7.0-alpine** (High-speed Spatial Key-Value Data Store)
* **PostgreSQL 15-alpine** (Relational Cold Storage Sink)
* **Docker & Docker Compose** (Containerization & Infrastructure Orchestration)

---

## 📂 Repository File Structure

```text
├── docker-compose.yml       # Orchestrates Kafka, Redis, and PostgreSQL containers
├── init.sql                 # Database relational schema initialization scripts
├── driver_service.py        # Real-time Location Engine (Redis Cache + Kafka Producer)
├── history_service.py       # Asynchronous Pipeline Ingestion Worker (Kafka Consumer -> Postgres)
└── README.md                # System architectural blueprint documentation
```

---

## ⚡ Getting Started

### 1. Prerequisites
Ensure you have the following installed natively on your local environment:
* [Docker Desktop](https://docker.com) (includes Docker Compose)
* [Python 3.10+](https://python.org)

### 2. Spanning the Infrastructure Layers
Spin up the isolated multi-container ecosystem in detached mode:
```bash
docker compose up -d
```

Verify that all baseline state stores and message brokers are fully initialized and healthy:
```bash
docker compose ps
```

### 3. Setting Up the Local Python Environment
Install the official low-level native protocol drivers required to communicate with Kafka, Redis, and PostgreSQL:

```bash
pip install redis kafka-python psycopg2-binary
```

### 4. Running the Event Ingestion Pipeline
Open a dedicated terminal window and spin up the asynchronous consumer daemon. This process remains idle, waiting to safely flush streaming events downstream into PostgreSQL:

```bash
python history_service.py
```

### 5. Simulating Real-Time Driver Operations
Open a secondary terminal window and execute the mock orchestration script. This simulates active drivers broadcasting continuous coordinates into São Paulo, Brazil, and tests the geometric matching algorithm:

```bash
python driver_service.py
```

---

## 📊 Verification & Data Testing

Upon a successful execution loop:
1. **Redis Cache:** Keeps memory footprints updated with the latest geographic spatial vectors.
2. **Kafka Engine:** Dispatches JSON data frames detailing precise coordinate changes.
3. **PostgreSQL Sink:** Captures the structural schemas automatically via `init.sql`.

To verify your cold storage database layer records directly inside the container, execute:
```bash
docker exec -it blueprint_postgres psql -U uber_admin -d ride_history_db -c "SELECT * FROM ride_history;"
```

---

## 🔒 Security & Performance Considerations

* **Decoupled Fallbacks:** In the event of a relational database bottleneck, Kafka securely retains state updates upstream, ensuring the core real-time app remains zero-downtime and highly resilient.
* **Production Adjustments:** For scaled deployments, swap standard environment variable strings for decoupled secret storage keys, adjust Kafka replication factors ($>1$), and partition tables inside the relational engine based on time frames.
