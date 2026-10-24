# mq-kafka-bridge

An MQ ↔ Kafka bridge that uses a **browse-then-delete** pattern to provide
durable, checkpointed consumption of IBM MQ queues via Kafka as the coordination
layer, with async cross-cluster replication via MirrorMaker 2.

---

## Architecture

```
Publisher app
     │
     ▼  PUT to INPUT.ALIAS (alias → topic ORDERS/INPUT)
 ┌─────────────────────────────────────────────────────────────┐
 │  SRCQM                                                      │
 │   ORDERS.INPUT (topic)                                      │
 │       │                                                     │
 │       ├─ SUB.ORDERS.HA1 → REMOTE.ORDERS.Q1 ──► XMITQ.CH1  │
 │       └─ SUB.ORDERS.HA2 → REMOTE.ORDERS.Q2 ──► XMITQ.CH2  │
 └───────────────────────────┬─────────────────────────────────┘
           SDR channel CH1   │   SDR channel CH2
           (port 1414)       │   (port 1415)   ← HA pair
                             ▼
 ┌─────────────────────────────────────────────────────────────┐
 │  TGTQM                                                      │
 │   ORDERS.INBOUND  ◄── FROM.SRCQM.CH1 / FROM.SRCQM.CH2      │
 │        │                                                    │
 │        │  (browse every 60 s)                               │
 └────────┼────────────────────────────────────────────────────┘
          │
          ▼
  ┌──────────────────────┐       ┌──────────────────────────────┐
  │  Browse Connector    │       │   PRIMARY Kafka Cluster      │
  │  (Python / pymqi)    │──────►│   topic: mq.orders.correlids │
  │  produces 1 msg/min  │       │   key = last CorrelId (hex)  │
  └──────────────────────┘       └──────────┬───────────────────┘
                                            │  MirrorMaker 2
                                            │  (async replication)
                                            ▼
                                 ┌──────────────────────────────┐
                                 │   SECONDARY Kafka Cluster    │
                                 │   topic: mq.orders.correlids │
                                 └──────────┬───────────────────┘
                                            │  (consume)
                                            ▼
                                  ┌──────────────────────┐
                                  │  Delete Connector    │
                                  │  (Python / pymqi)    │
                                  │  destructively GETs  │
                                  │  all msgs BEFORE     │
                                  │  target CorrelId     │
                                  └──────────────────────┘
```

### Key design decisions

| Concern | Decision |
|---------|----------|
| HA channel routing | Two independent SDR/RCVR channel pairs on separate listener ports. Both deliver to the same `ORDERS.INBOUND` queue on TGTQM. |
| Browse granularity | One Kafka message per 60 s cycle carrying the CorrelId of the **last** (most recent) message in the queue. |
| Delete semantics | The delete connector removes every message *before* (not including) the message with the published CorrelId. The checkpoint message and everything after it remain intact. |
| Kafka offset commit | Manual — only committed after all MQ deletes succeed. Kafka will redeliver on restart if the delete connector crashes mid-flight. |
| Cross-cluster async | MirrorMaker 2 with `IdentityReplicationPolicy` so topic names are preserved on the secondary cluster. |
| Single-replica connectors | Both Python connectors run as `replicas: 1`. The browse connector has no concurrency need; the delete connector uses `MQOO_INPUT_EXCLUSIVE` which prevents duplicate execution. |
| Throttle | The delete connector polls `GET /throttle` every 10 s. `enabled=false` pauses it; `rate_per_min` adds a sleep of `60/rate` seconds between cycles. State is in-process in the API (resets on restart). |
| Frontend | FastAPI serves the built React bundle as static files in production. In dev, Vite's dev server proxies API calls to FastAPI. |

---

## Project layout

```
mq-kafka-bridge/
├── config/
│   ├── api.env                  # env vars for FastAPI service
│   ├── browse_connector.env     # env vars for browse connector
│   └── delete_connector.env     # env vars for delete connector
├── k8s/
│   ├── namespace.yaml
│   ├── kafka/
│   │   ├── primary-cluster.yaml   # Strimzi Kafka (primary)
│   │   └── secondary-cluster.yaml # Strimzi Kafka (secondary)
│   ├── mm2/
│   │   └── mirrormaker2.yaml      # KafkaMirrorMaker2 resource
│   ├── mq/
│   │   ├── srcqm.yaml             # SRCQM StatefulSet + Service
│   │   └── tgtqm.yaml             # TGTQM StatefulSet + Service
│   └── connectors/
│       ├── browse-connector-deployment.yaml
│       ├── delete-connector-deployment.yaml
│       └── api-ui-deployment.yaml  # FastAPI + React UI + nginx proxy config
├── mqsc/
│   ├── source_qmgr.mqsc    # annotated MQSC for SRCQM
│   └── target_qmgr.mqsc    # annotated MQSC for TGTQM
├── src/
│   ├── api/
│   │   ├── Dockerfile
│   │   ├── main.py          # FastAPI — /messages, /queue/depth, /throttle
│   │   └── requirements.txt
│   ├── browse_connector/
│   │   ├── Dockerfile
│   │   ├── main.py
│   │   └── requirements.txt
│   ├── delete_connector/
│   │   ├── Dockerfile
│   │   ├── main.py          # throttle-aware via GET /throttle polling
│   │   └── requirements.txt
│   └── ui/
│       ├── Dockerfile
│       ├── index.html
│       ├── package.json
│       ├── tsconfig.json
│       ├── vite.config.ts
│       └── src/
│           ├── App.tsx      # message composer + throttle panel + queue depth
│           ├── index.css
│           └── main.tsx
└── docker-compose.yml       # local dev stack (all services)
```

---

## Prerequisites

- Docker / `docker compose` (local dev)
- Node.js 20+ (for local UI dev without Docker)
- Kubernetes cluster with [Strimzi operator](https://strimzi.io/) installed
- IBM MQ container image access (`icr.io/ibm-messaging/mq`)
- Python 3.12+ (if running connectors outside Docker)

---

## Local development

```bash
# Start the full stack (MQ, Kafka, API, UI, connectors)
docker compose up --build

# Open the UI
open http://localhost:5173

# Use the UI to:
#   • Compose and send messages to INPUT.ALIAS on SRCQM
#   • Watch ORDERS.INBOUND depth update every 10 s
#   • Pause / resume the delete connector with the toggle
#   • Set a rate limit (1–60 cycles/min) on the delete connector

# Publish a test message to the source alias queue
# (requires MQ client or runmqsc on the srcqm container)
docker exec -it mq-kafka-bridge-srcqm-1 bash
$ runmqsc SRCQM
> AMQSPUT INPUT.ALIAS SRCQM

# Watch the browse connector logs
docker compose logs -f browse-connector

# Watch the delete connector logs
docker compose logs -f delete-connector
```

---

## Kubernetes deployment

```bash
# 1. Install Strimzi operator in each namespace
kubectl create namespace kafka-primary kafka-secondary mq-kafka-bridge
helm install strimzi-primary strimzi/strimzi-kafka-operator -n kafka-primary
helm install strimzi-secondary strimzi/strimzi-kafka-operator -n kafka-secondary

# 2. Deploy Kafka clusters
kubectl apply -f k8s/kafka/primary-cluster.yaml
kubectl apply -f k8s/kafka/secondary-cluster.yaml

# 3. Deploy MirrorMaker 2
kubectl apply -f k8s/mm2/mirrormaker2.yaml

# 4. Deploy MQ queue managers
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/mq/srcqm.yaml
kubectl apply -f k8s/mq/tgtqm.yaml

# 5. Update image references in connector deployments, then:
kubectl apply -f k8s/connectors/
```

---

## Configuration reference

### Browse connector

| Env var | Default | Description |
|---------|---------|-------------|
| `MQ_HOST` | — | Hostname of TGTQM |
| `MQ_PORT` | `1414` | MQ listener port |
| `MQ_QMGR` | — | Queue manager name |
| `MQ_QUEUE` | `ORDERS.INBOUND` | Queue to browse |
| `MQ_CHANNEL` | `PYTHON.SVRCONN` | SVRCONN channel name |
| `KAFKA_BOOTSTRAP_SERVERS` | — | Primary cluster bootstrap |
| `KAFKA_TOPIC` | `mq.orders.correlids` | Target Kafka topic |
| `BROWSE_INTERVAL_SECONDS` | `60` | Browse cycle period |

### Delete connector

| Env var | Default | Description |
|---------|---------|-------------|
| `MQ_HOST` | — | Hostname of TGTQM |
| `MQ_QUEUE` | `ORDERS.INBOUND` | Queue to purge |
| `KAFKA_BOOTSTRAP_SERVERS` | — | **Secondary** cluster bootstrap |
| `KAFKA_TOPIC` | `mq.orders.correlids` | Source Kafka topic (replicated by MM2) |
| `KAFKA_GROUP_ID` | `mq-delete-connector` | Consumer group (synced by MM2 checkpoints) |

---

## Message flow walkthrough

1. **Producer** does `MQPUT` to `INPUT.ALIAS` on SRCQM.
2. MQ pub/sub routes the message onto the `ORDERS/INPUT` topic.
3. Both subscriptions (`SUB.ORDERS.HA1`, `SUB.ORDERS.HA2`) receive a copy and deliver it to `ORDERS.INBOUND` on TGTQM via their respective SDR channels.
4. Every 60 s, the **browse connector** opens `ORDERS.INBOUND` with `MQGMO_BROWSE_*`, walks the entire queue, records the `CorrelId` of the last message, and produces one Kafka message to the primary cluster topic with that CorrelId as the key.
5. **MirrorMaker 2** asynchronously replicates the Kafka message to the secondary cluster.
6. The **delete connector** consumes the message from the secondary cluster, opens `ORDERS.INBOUND` with `MQOO_INPUT_EXCLUSIVE`, browses forward until it finds the target CorrelId, then destructively GETs every message *before* that position.
7. The Kafka offset is committed only after all MQ GETs succeed — guaranteeing at-least-once delete semantics.
