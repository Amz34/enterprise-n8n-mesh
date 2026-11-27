# System Architecture & Technical Specifications

## 1. Execution Queue Engine (BullMQ + Redis)

In standard n8n deployments, workflow executions run inside the main process memory space. If a long-running web scraping or data aggregation job consumes excessive CPU or experiences an out-of-memory event, the entire n8n service terminates, dropping incoming webhooks and corrupting in-flight SQLite state.

The **Enterprise n8n Mesh** decouples execution into a distributed producer-consumer topology:

```
[ Incoming Webhook ]
       │
       ▼
[ n8n-automation (Primary) ]
       │
       ▼  Enqueues task (BullMQ job)
[ Redis 7 Cluster / Single-Node with AOF ]
       │
       ▼  Pulls next available task (Atomic BRPOPLPUSH)
[ n8n-worker Fleet (Node 1..N) ]
       │
       ▼  Writes execution trace & logs
[ PostgreSQL 16 (ACID Relational Storage) ]
```

### Queue Characteristics
- **Queue Engine**: BullMQ 5.x on top of Redis 7.
- **Job Retention**: Completed jobs are purged or archived according to `EXECUTIONS_DATA_PRUNE=true` with a 7-day retention window (`EXECUTIONS_DATA_MAX_AGE=168`).
- **Worker Concurrency**: Set to 10 concurrent jobs per worker container via `N8N_WORKERS_CONCURRENCY=10`.

---

## 2. PostgreSQL 16 Relational Storage Engine

### Database Topology
- **Database**: `n8n_enterprise`
- **Engine**: PostgreSQL 16.x with UTF-8 encoding.
- **Connection Pooling**: Managed with connection pooling limits to avoid exhaustion during spikes.
- **Backup Mechanism**: Physical/Logical consistent backup via `pg_dump -Fc` snapshots, ensuring point-in-time recovery without database read locks.

---

## 3. Ingress Security & TLS Termination

All external webhooks route through Caddy:
- Strict TLS 1.3 encryption.
- Direct path routing for webhooks (`/webhook/*`).
- Client IP forwarding headers:
  - `X-Forwarded-For`
  - `X-Real-IP`
  - `X-Forwarded-Proto`
- Editor access restricted to internal Tailscale mesh (`100.x.y.z`) and localhost.

---

## 4. Disaster Recovery & SRE Runbook

### Automatic Watchdog
The included `n8n_infra_watchdog.py` runs on a recurring schedule to monitor:
1. HTTP endpoint availability (`/healthz`).
2. PostgreSQL database connectivity and database size.
3. Execution failure rates in the preceding 24 hours.
4. Container health states across the fleet.

### Restoration Procedure
To restore a snapshot:
```bash
pg_restore -h $DB_HOST -p $DB_PORT -U $DB_USER -d n8n_enterprise -c -v /path/to/n8n_backup_latest.dump
```
