"""
api/main.py
───────────
FastAPI service for the 3-qmgr topology:

  PROXYQM  — receives app PUTs via INPUT.ALIAS → topic → subs → QMGRA + QMGRB
  QMGRA    — UI-triggered consume; each consumed message is published to Kafka
  QMGRB    — delete connector mirrors QMGRA consumption via Kafka CorrelId events

Endpoints:
  POST /messages          — PUT a message to INPUT.ALIAS on PROXYQM
  GET  /queue/depth/a     — MQINQ depth of ORDERS.INBOUND on QMGRA
  GET  /queue/depth/b     — MQINQ depth of ORDERS.INBOUND on QMGRB
  GET  /queue/depth       — both depths in one response
  POST /queue/a/consume   — destructively GET all messages from QMGRA; publishes
                            one Kafka event per message to mq.orders.consumed
  GET  /events/delete     — SSE stream; delete connector POSTs events here via /events/delete/publish
  POST /events/delete/publish — internal endpoint called by delete connector to push SSE events
  GET  /throttle          — current throttle config (delete connector polls this)
  PUT  /throttle          — update throttle config (UI calls this)
  GET  /health            — liveness probe
"""

import asyncio
import json
import logging
import os
import time
from typing import Annotated

import pymqi
from confluent_kafka import Producer as KafkaProducer
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [api] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────
# PROXYQM — app PUTs land here
PROXYQM_HOST    = os.environ["PROXYQM_HOST"]
PROXYQM_PORT    = int(os.environ.get("PROXYQM_PORT", "1414"))
PROXYQM_QMGR    = os.environ.get("PROXYQM_QMGR", "PROXYQM")
PROXYQM_CHANNEL = os.environ.get("PROXYQM_CHANNEL", "APP.SVRCONN")
PROXYQM_USER    = os.environ.get("PROXYQM_USER", "")
PROXYQM_PASS    = os.environ.get("PROXYQM_PASSWORD", "")
ALIAS_QUEUE     = os.environ.get("ALIAS_QUEUE", "INPUT.ALIAS")

# QMGRA — browse connector reads from here
QMGRA_HOST    = os.environ["QMGRA_HOST"]
QMGRA_PORT    = int(os.environ.get("QMGRA_PORT", "1414"))
QMGRA_QMGR    = os.environ.get("QMGRA_QMGR", "QMGRA")
QMGRA_CHANNEL = os.environ.get("QMGRA_CHANNEL", "BROWSE.SVRCONN")
QMGRA_USER    = os.environ.get("QMGRA_USER", "")
QMGRA_PASS    = os.environ.get("QMGRA_PASSWORD", "")
QMGRA_QUEUE   = os.environ.get("QMGRA_QUEUE", "ORDERS.INBOUND")

# QMGRB — delete connector targets this
QMGRB_HOST    = os.environ["QMGRB_HOST"]
QMGRB_PORT    = int(os.environ.get("QMGRB_PORT", "1414"))
QMGRB_QMGR    = os.environ.get("QMGRB_QMGR", "QMGRB")
QMGRB_CHANNEL = os.environ.get("QMGRB_CHANNEL", "DELETE.SVRCONN")
QMGRB_USER    = os.environ.get("QMGRB_USER", "")
QMGRB_PASS    = os.environ.get("QMGRB_PASSWORD", "")
QMGRB_QUEUE   = os.environ.get("QMGRB_QUEUE", "ORDERS.INBOUND")

# Kafka — publish consumed CorrelIds so the delete connector can mirror on QMGRB
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")
KAFKA_TOPIC     = os.environ.get("KAFKA_CONSUME_TOPIC", "mq.orders.consumed")

# ── Throttle state (in-process) ────────────────────────────────────────────────
_throttle: dict = {
    "enabled": True,
    "rate_per_min": 0,
    "updated_at": time.time(),
}

# ── SSE event bus for delete connector activity ────────────────────────────────
# Each connected browser gets its own asyncio.Queue added here.
_delete_event_subscribers: list[asyncio.Queue] = []


def _publish_delete_event(event: dict) -> None:
    """Broadcast a delete-connector event to all SSE subscribers."""
    data = json.dumps(event)
    for q in _delete_event_subscribers:
        try:
            q.put_nowait(data)
        except asyncio.QueueFull:
            pass  # slow subscriber — drop rather than block


# ── Kafka producer (lazily initialised) ───────────────────────────────────────
_kafka_producer: KafkaProducer | None = None


def _get_kafka_producer() -> KafkaProducer | None:
    """Return the shared Kafka producer, or None if KAFKA_BOOTSTRAP_SERVERS is unset."""
    global _kafka_producer
    if not KAFKA_BOOTSTRAP:
        return None
    if _kafka_producer is None:
        _kafka_producer = KafkaProducer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
            "retry.backoff.ms": 500,
        })
        log.info("Kafka producer initialised — bootstrap=%s topic=%s", KAFKA_BOOTSTRAP, KAFKA_TOPIC)
    return _kafka_producer


# ── MQ helpers ─────────────────────────────────────────────────────────────────
def _connect(host: str, port: int, qmgr_name: str, channel: str,
             user: str, password: str) -> pymqi.QueueManager:
    cd = pymqi.CD()
    cd.ChannelName = channel.encode()
    cd.ConnectionName = f"{host}({port})".encode()
    cd.ChannelType = pymqi.CMQC.MQCHT_CLNTCONN
    cd.TransportType = pymqi.CMQC.MQXPT_TCP
    qmgr = pymqi.QueueManager(None)
    if user:
        qmgr.connect_with_options(
            qmgr_name, cd=cd,
            opts=pymqi.CMQC.MQCNO_CLIENT_BINDING,
            user=user.encode(), password=password.encode(),
        )
    else:
        qmgr.connect_with_options(qmgr_name, cd=cd,
                                  opts=pymqi.CMQC.MQCNO_CLIENT_BINDING)
    return qmgr


def _mqinq_depth(host: str, port: int, qmgr_name: str, channel: str,
                 user: str, password: str, queue_name: str) -> int:
    """Open a queue with MQOO_INQUIRE and return MQIA_CURRENT_Q_DEPTH."""
    qmgr = _connect(host, port, qmgr_name, channel, user, password)
    try:
        q = pymqi.Queue(qmgr, queue_name,
                        pymqi.CMQC.MQOO_INQUIRE | pymqi.CMQC.MQOO_FAIL_IF_QUIESCING)
        try:
            return q.inquire(pymqi.CMQC.MQIA_CURRENT_Q_DEPTH)
        finally:
            q.close()
    finally:
        try:
            qmgr.disconnect()
        except Exception:
            pass


def _consume_all(host: str, port: int, qmgr_name: str, channel: str,
                 user: str, password: str, queue_name: str) -> int:
    """Destructively GET every message from queue_name. Returns count consumed."""
    qmgr = _connect(host, port, qmgr_name, channel, user, password)
    gmo = pymqi.GMO()
    gmo.Options = pymqi.CMQC.MQGMO_NO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING
    gmo.MatchOptions = pymqi.CMQC.MQMO_NONE
    count = 0
    try:
        q = pymqi.Queue(
            qmgr, queue_name,
            pymqi.CMQC.MQOO_INPUT_EXCLUSIVE | pymqi.CMQC.MQOO_FAIL_IF_QUIESCING,
        )
        try:
            while True:
                md = pymqi.MD()
                try:
                    q.get(None, md, gmo)
                    count += 1
                except pymqi.MQMIError as e:
                    if e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                        break
                    raise
        finally:
            q.close()
    finally:
        try:
            qmgr.disconnect()
        except Exception:
            pass
    return count


def _consume_and_publish(host: str, port: int, qmgr_name: str, channel: str,
                         user: str, password: str, queue_name: str) -> int:
    """
    Destructively GET every message from queue_name and publish one Kafka event
    per message to KAFKA_TOPIC with the message's CorrelId as the key.
    Returns the count consumed.
    """
    producer = _get_kafka_producer()
    qmgr = _connect(host, port, qmgr_name, channel, user, password)
    gmo = pymqi.GMO()
    gmo.Options = pymqi.CMQC.MQGMO_NO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING
    gmo.MatchOptions = pymqi.CMQC.MQMO_NONE
    count = 0
    try:
        q = pymqi.Queue(
            qmgr, queue_name,
            pymqi.CMQC.MQOO_INPUT_EXCLUSIVE | pymqi.CMQC.MQOO_FAIL_IF_QUIESCING,
        )
        try:
            while True:
                md = pymqi.MD()
                try:
                    q.get(None, md, gmo)
                except pymqi.MQMIError as e:
                    if e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                        break
                    raise
                count += 1
                correl_hex = bytes(md.CorrelId).hex()
                msg_id_hex = bytes(md.MsgId).hex()
                if producer:
                    payload = json.dumps({
                        "queue": queue_name,
                        "qmgr": qmgr_name,
                        "correl_id": correl_hex,
                        "msg_id": msg_id_hex,
                        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    }).encode()
                    producer.produce(
                        topic=KAFKA_TOPIC,
                        key=correl_hex.encode(),
                        value=payload,
                    )
                    log.info("Consumed + published: correl_id=%s", correl_hex)
                else:
                    log.info("Consumed (no Kafka): correl_id=%s", correl_hex)
        finally:
            q.close()
            if producer and count > 0:
                producer.flush(timeout=30)
    finally:
        try:
            qmgr.disconnect()
        except Exception:
            pass
    return count


# ── Pydantic models ────────────────────────────────────────────────────────────
class MessageRequest(BaseModel):
    payload: str = Field(..., description="Message body (UTF-8 text)")
    correl_id: str | None = Field(
        None,
        description="Optional CorrelId as a 48-char hex string (24 bytes). "
                    "Omit for zero CorrelId.",
    )
    format: str = Field("MQSTR", description="MQMD Format field value")
    persistence: int = Field(
        pymqi.CMQC.MQPER_PERSISTENT,
        description="0=not persistent, 1=persistent (default)",
    )


class MessageResponse(BaseModel):
    msg_id: str
    correl_id: str
    queue: str
    qmgr: str


class QueueDepthOne(BaseModel):
    queue: str
    qmgr: str
    depth: int


class QueueDepthBoth(BaseModel):
    qmgra: QueueDepthOne
    qmgrb: QueueDepthOne


class ThrottleConfig(BaseModel):
    enabled: bool = Field(True, description="When false, delete connector pauses")
    rate_per_min: Annotated[int, Field(ge=0, le=60)] = Field(
        0, description="Max delete-cycles per minute. 0 = unlimited."
    )


class ThrottleResponse(ThrottleConfig):
    updated_at: float


class ConsumeResponse(BaseModel):
    queue: str
    qmgr: str
    consumed: int


class DeleteEvent(BaseModel):
    correl_id: str  # "n/a" for depth-based deletes
    partition: int
    offset: int
    deleted: int
    timestamp_utc: str
    depth: int = 0


# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="MQ-Kafka Bridge API",
    description=(
        "3-qmgr topology: PUT to PROXYQM INPUT.ALIAS, "
        "browse QMGRA, delete from QMGRB."
    ),
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Routes ─────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/messages", response_model=MessageResponse, status_code=status.HTTP_201_CREATED)
def put_message(req: MessageRequest):
    """PUT a message to INPUT.ALIAS on PROXYQM.  Pub/sub routes it to QMGRA and QMGRB."""
    try:
        qmgr = _connect(PROXYQM_HOST, PROXYQM_PORT, PROXYQM_QMGR,
                        PROXYQM_CHANNEL, PROXYQM_USER, PROXYQM_PASS)
    except pymqi.MQMIError as e:
        log.error("Cannot connect to PROXYQM: %s", e)
        raise HTTPException(status_code=503, detail=f"Cannot connect to PROXYQM: {e.reason}")

    try:
        md = pymqi.MD()
        md.Format = req.format.encode().ljust(8)[:8]
        md.Persistence = req.persistence
        if req.correl_id:
            raw = bytes.fromhex(req.correl_id)
            if len(raw) != 24:
                raise HTTPException(status_code=422,
                                    detail="correl_id must be exactly 48 hex chars (24 bytes)")
            md.CorrelId = raw

        pmo = pymqi.PMO(Options=pymqi.CMQC.MQPMO_NEW_MSG_ID)
        q = pymqi.Queue(qmgr, ALIAS_QUEUE, pymqi.CMQC.MQOO_OUTPUT)
        try:
            q.put(req.payload.encode(), md, pmo)
        finally:
            q.close()

        msg_id_hex    = bytes(md.MsgId).hex()
        correl_id_hex = bytes(md.CorrelId).hex()
        log.info("PUT to %s msgId=%s correlId=%s", ALIAS_QUEUE, msg_id_hex, correl_id_hex)
        return MessageResponse(
            msg_id=msg_id_hex,
            correl_id=correl_id_hex,
            queue=ALIAS_QUEUE,
            qmgr=PROXYQM_QMGR,
        )
    except pymqi.MQMIError as e:
        log.error("MQPUT failed: %s", e)
        raise HTTPException(status_code=500, detail=f"MQPUT failed: {e.reason}")
    finally:
        try:
            qmgr.disconnect()
        except Exception:
            pass


@app.get("/queue/depth/a", response_model=QueueDepthOne)
def depth_a():
    """Depth of ORDERS.INBOUND on QMGRA (browse connector reads from here)."""
    try:
        d = _mqinq_depth(QMGRA_HOST, QMGRA_PORT, QMGRA_QMGR,
                         QMGRA_CHANNEL, QMGRA_USER, QMGRA_PASS, QMGRA_QUEUE)
        return QueueDepthOne(queue=QMGRA_QUEUE, qmgr=QMGRA_QMGR, depth=d)
    except pymqi.MQMIError as e:
        raise HTTPException(status_code=500, detail=f"QMGRA depth failed: {e.reason}")


@app.get("/queue/depth/b", response_model=QueueDepthOne)
def depth_b():
    """Depth of ORDERS.INBOUND on QMGRB (delete connector targets this)."""
    try:
        d = _mqinq_depth(QMGRB_HOST, QMGRB_PORT, QMGRB_QMGR,
                         QMGRB_CHANNEL, QMGRB_USER, QMGRB_PASS, QMGRB_QUEUE)
        return QueueDepthOne(queue=QMGRB_QUEUE, qmgr=QMGRB_QMGR, depth=d)
    except pymqi.MQMIError as e:
        raise HTTPException(status_code=500, detail=f"QMGRB depth failed: {e.reason}")


@app.get("/queue/depth", response_model=QueueDepthBoth)
def depth_both():
    """Both queue depths in one call."""
    errors: list[str] = []

    da = None
    try:
        da = _mqinq_depth(QMGRA_HOST, QMGRA_PORT, QMGRA_QMGR,
                          QMGRA_CHANNEL, QMGRA_USER, QMGRA_PASS, QMGRA_QUEUE)
    except pymqi.MQMIError as e:
        errors.append(f"QMGRA: {e.reason}")

    db = None
    try:
        db = _mqinq_depth(QMGRB_HOST, QMGRB_PORT, QMGRB_QMGR,
                          QMGRB_CHANNEL, QMGRB_USER, QMGRB_PASS, QMGRB_QUEUE)
    except pymqi.MQMIError as e:
        errors.append(f"QMGRB: {e.reason}")

    if errors:
        raise HTTPException(status_code=500, detail="; ".join(errors))

    return QueueDepthBoth(
        qmgra=QueueDepthOne(queue=QMGRA_QUEUE, qmgr=QMGRA_QMGR, depth=da),
        qmgrb=QueueDepthOne(queue=QMGRB_QUEUE, qmgr=QMGRB_QMGR, depth=db),
    )


@app.post("/queue/a/consume", response_model=ConsumeResponse)
def consume_qmgra():
    """Destructively GET all messages from ORDERS.INBOUND on QMGRA and publish
    one Kafka event per message so the delete connector mirrors onto QMGRB."""
    try:
        n = _consume_and_publish(QMGRA_HOST, QMGRA_PORT, QMGRA_QMGR,
                                 QMGRA_CHANNEL, QMGRA_USER, QMGRA_PASS, QMGRA_QUEUE)
        log.info("Consumed %d messages from QMGRA %s", n, QMGRA_QUEUE)
        return ConsumeResponse(queue=QMGRA_QUEUE, qmgr=QMGRA_QMGR, consumed=n)
    except pymqi.MQMIError as e:
        raise HTTPException(status_code=500, detail=f"QMGRA consume failed: {e.reason}")


@app.post("/queue/b/consume", response_model=ConsumeResponse)
def consume_qmgrb():
    """Destructively GET all messages from ORDERS.INBOUND on QMGRB."""
    try:
        n = _consume_all(QMGRB_HOST, QMGRB_PORT, QMGRB_QMGR,
                         QMGRB_CHANNEL, QMGRB_USER, QMGRB_PASS, QMGRB_QUEUE)
        log.info("Consumed %d messages from QMGRB %s", n, QMGRB_QUEUE)
        return ConsumeResponse(queue=QMGRB_QUEUE, qmgr=QMGRB_QMGR, consumed=n)
    except pymqi.MQMIError as e:
        raise HTTPException(status_code=500, detail=f"QMGRB consume failed: {e.reason}")


@app.get("/events/delete")
async def delete_events(request: Request):
    """SSE stream — push an event each time the delete connector processes a Kafka message."""
    q: asyncio.Queue = asyncio.Queue(maxsize=50)
    _delete_event_subscribers.append(q)

    async def stream():
        try:
            # Send an initial ping so the browser knows the connection is live
            yield "event: connected\ndata: {}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    data = await asyncio.wait_for(q.get(), timeout=15.0)
                    yield f"event: delete\ndata: {data}\n\n"
                except asyncio.TimeoutError:
                    # Keepalive comment — prevents proxies from closing the connection
                    yield ": keepalive\n\n"
        finally:
            _delete_event_subscribers.remove(q)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # disable nginx buffering
        },
    )


@app.post("/events/delete/publish", status_code=status.HTTP_204_NO_CONTENT)
def publish_delete_event(evt: DeleteEvent):
    """Called by the delete connector after each successful Kafka-triggered delete cycle."""
    _publish_delete_event(evt.model_dump())
    log.info("Delete event published: correl_id=%s deleted=%d", evt.correl_id, evt.deleted)


@app.get("/throttle", response_model=ThrottleResponse)
def get_throttle():
    """Return current throttle config. The delete connector polls this."""
    return ThrottleResponse(**_throttle)


@app.put("/throttle", response_model=ThrottleResponse)
def set_throttle(cfg: ThrottleConfig):
    """Update throttle config from the UI."""
    _throttle["enabled"]      = cfg.enabled
    _throttle["rate_per_min"] = cfg.rate_per_min
    _throttle["updated_at"]   = time.time()
    log.info("Throttle updated: enabled=%s rate_per_min=%d",
             cfg.enabled, cfg.rate_per_min)
    return ThrottleResponse(**_throttle)


# ── Serve built UI from /ui (production) ──────────────────────────────────────
import pathlib
_ui_dist = pathlib.Path(__file__).parent.parent / "ui" / "dist"
if _ui_dist.exists():
    app.mount("/", StaticFiles(directory=str(_ui_dist), html=True), name="ui")
