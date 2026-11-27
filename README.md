# Enterprise n8n Mesh (Production Architecture)

An enterprise-grade, high-availability workflow automation architecture running **n8n in Queue Mode** with **PostgreSQL 16**, **Redis 7 (BullMQ)**, decoupled worker fleets, TLS edge termination, and automated SRE observability.

---

## Architecture Overview

```
                      +-----------------------------+
                      |       Public Ingress        |
                      |   (Caddy TLS Reverse Proxy)  |
                      +--------------+--------------+
                                     |
                                     v
                      +-----------------------------+
                      |   n8n Primary (Main Node)   |
                      |   - Webhook Receiver        |
                      |   - UI & Execution Engine   |
                      +-------+-------------+-------+
                              |             |
        Job Enqueue (BullMQ)  |             |  State & Workflows
                              v             v
       +------------------------+         +--------------------------+
       |   Redis 7 (In-Memory)  |         |   PostgreSQL 16 (RDBMS)  |
       |   Queue & Event Store  |         |   Relational Persistence |
       +-----------+------------+         +------------+-------------+
                   |                                   |
                   | Work Fetch (BRPOPLPUSH)           |
                   v                                   |
       +------------------------+                      |
       |  n8n Worker Fleet      |                      |
       |  (Decoupled Scale-out) +----------------------+
       |  Concurrency: 10/host  |   Read/Write Workflow Data
       +------------------------+
```

---

## Key System Properties

| Dimension | Standard Community Setup | Enterprise Mesh Setup |
| :--- | :--- | :--- |
| **Storage Backend** | SQLite file lock | PostgreSQL 16 ACID Transaction Engine |
| **Execution Mode** | Synchronous In-Process (`regular`) | BullMQ Distributed Queue (`queue`) |
| **Worker Decoupling** | Monolithic (UI + Webhook + Worker) | Separated Primary Node + Dedicated Workers |
| **Crash Protection** | Worker crash kills editor & webhooks | Worker crash is isolated; jobs retry cleanly |
| **Ingress Security** | Direct raw HTTP port exposure | TLS reverse proxy, header filtering, mTLS ready |
| **Failure Recovery** | Manual restart & inspection | Global Error Workflow + Telegram SRE Alerts |
| **Backup Integrity** | File copy prone to lock errors | Atomic `pg_dump -Fc` with transaction snapshots |

---

## Components

1. **n8n Primary Engine (`n8n-automation`)**:
   - Manages workflow orchestration, credentials, canvas UI, and webhook ingress.
   - Enqueues background tasks onto Redis BullMQ queues without blocking HTTP listeners.

2. **n8n Worker Pool (`n8n-worker`)**:
   - Dedicated headless execution instances with concurrency controls.
   - Scales horizontally across nodes by adding worker containers pointing to the shared Redis and PostgreSQL cluster.

3. **Redis 7 (`hermes-redis`)**:
   - High-throughput BullMQ queue backend storing execution job payloads and execution locks.

4. **PostgreSQL 16 (`centralised_agent_pgvector`)**:
   - Stores workflow definitions, credentials, execution history, and audit logs.

5. **Apify Enterprise Integration**:
   - High-volume web extraction and browser automation integration via REST v2 with retry backoff and error trapping.

6. **Automated Watchdog & Backup Daemon**:
   - Cron-managed health monitor probing database latency, Redis connection health, failed executions in last 24h, and disk consumption.

---

## Quickstart

### 1. Configuration
Copy the sample environment file:
```bash
cp .env.example .env
```
Fill in the database credentials, Redis configuration, encryption key, and Telegram alerting parameters.

### 2. Deploy Infrastructure
```bash
docker compose -f docker-compose.enterprise.yml up -d
```

### 3. Verify Health
```bash
curl -f http://127.0.0.1:5678/healthz
```

### 4. Continuous Workflow Backup & Sync
```bash
# Decompose active workflows into git
./scripts/git_sync_workflows.sh

# Run atomic PostgreSQL backup
./scripts/backup_n8n.sh
```

---

## Incident & Error Handling Policy

- Every workflow inherits from the **Global Error Trigger Mesh** (`N8N_DEFAULT_ERROR_WORKFLOW_ID`).
- Any unhandled node failure dispatches an immediate incident payload to the configured SRE webhook or Telegram alerting bot.
- Execution traces retain input payloads for idempotent replay.

---

## License
Apache-2.0. See [LICENSE](LICENSE) for details.
