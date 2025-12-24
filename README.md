# Symphony 🎼

**Symphony** is a lightweight container and job orchestrator.

A central **Conductor** schedules Docker and exec jobs across distributed **Nodes**, balancing workloads using **virtual resource capacities** instead of raw CPU or memory.

---

## Architecture

```
User / CLI
     |
     v
+-------------+
|  Conductor  |
+-------------+
       |
       | secure persistent connection (TLS)
       |
+-------------+     +-------------+
|    Node     |     |    Node     |
| (multiple)  | ... | (multiple)  |
+-------------+     +-------------+
```

---

## Core Concepts

### Conductor
The central controller responsible for:
- node registry and health
- capacity tracking
- job scheduling
- job lifecycle management

### Nodes
Nodes are clients that connect to the Conductor and can run multiple applications.

Each Node declares:
- one or more **groups** (e.g. `gpu`, `cpu`, `edge-bkk`)
- **virtual capacity classes**, for example:
  ```
  A = 100
  B = 200
  ```

### Jobs
Jobs are units of work submitted to the Conductor.

When submitting a job, you specify how much capacity it consumes:
```
A10
A10,B20
```

The Conductor schedules the job to an eligible Node and balances workloads automatically.

---

## Features (v1)

- Single Conductor
- Multiple Nodes
- Node groups
- Virtual resource classes (A/B/…)
- Docker jobs
- Exec jobs
- Capacity-aware scheduling
- Balanced distribution
- Heartbeats and health checks
- Job logs and exit codes

---

## CLI Usage

### Start the Conductor

```bash
symphony conductor start \
  --listen 0.0.0.0:8080 \
  --state ./state
```

### Start a Node

```bash
symphony node start \
  --conductor https://CONDUCTOR:8080 \
  --node-id node-01 \
  --groups gpu,edge-bkk \
  --capacity A=100 \
  --capacity B=200
```

Nodes connect outbound only; no public IP is required.

---

### Submit a Docker Job

```bash
symphony job submit docker \
  --group gpu \
  --usage A10,B20 \
  --image registry.example.com/myapp:1.0 \
  --env ENV=prod
```

---

### Submit an Exec Job

```bash
symphony job submit exec \
  --group edge-bkk \
  --usage A5 \
  --command "/usr/bin/python3" \
  --arg "main.py"
```

---

### Inspect Jobs and Nodes

```bash
symphony job get <JOB_ID>
symphony job logs <JOB_ID> --follow

symphony node list
symphony node get node-01
```

---

### Cancel a Job

```bash
symphony job cancel <JOB_ID>
```

---

## API (v1)

The Conductor exposes a simple HTTP API:

- `POST /v1/jobs`
- `GET /v1/jobs/{id}`
- `POST /v1/jobs/{id}/cancel`
- `GET /v1/nodes`
- `GET /v1/nodes/{id}`

Node–Conductor communication uses a secure persistent channel
(WebSocket or gRPC over TLS).
