"""
delete_connector/main.py
────────────────────────
Consumes messages from the Kafka topic published by the browse connector.
For each Kafka message received:

  1. Decode the CorrelId published when a message was consumed from QMGRA
  2. Fetch the current throttle config from the API — pause or rate-limit
  3. Open ORDERS.INBOUND on QMGRB
  4. Destructively GET the message whose CorrelId matches the target
  5. Log the outcome and commit the Kafka offset

This mirrors QMGRA consumption onto QMGRB: every message consumed from QMGRA
triggers an identical delete from QMGRB by CorrelId.

Environment variables:
  MQ_HOST, MQ_PORT, MQ_QMGR, MQ_QUEUE, MQ_CHANNEL, MQ_USER, MQ_PASSWORD
  KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_GROUP_ID,
  KAFKA_POLL_TIMEOUT_SECONDS
  THROTTLE_API_URL         — e.g. http://api:8000/throttle (optional)
  THROTTLE_POLL_INTERVAL_S — how often to re-fetch throttle config (default 10)
  EVENT_API_URL            — e.g. http://api:8000/events/delete/publish (optional)
"""

import json
import logging
import os
import time
import urllib.error
import urllib.request

import pymqi
from confluent_kafka import Consumer, KafkaError, KafkaException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [delete-connector] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────
MQ_HOST = os.environ["MQ_HOST"]
MQ_PORT = int(os.environ.get("MQ_PORT", "1414"))
MQ_QMGR = os.environ["MQ_QMGR"]
MQ_QUEUE = os.environ.get("MQ_QUEUE", "ORDERS.INBOUND")
MQ_CHANNEL = os.environ.get("MQ_CHANNEL", "PYTHON.SVRCONN")
MQ_USER = os.environ.get("MQ_USER", "")
MQ_PASSWORD = os.environ.get("MQ_PASSWORD", "")

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "mq.orders.consumed")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "mq-delete-connector")
KAFKA_POLL_TIMEOUT = float(os.environ.get("KAFKA_POLL_TIMEOUT_SECONDS", "5.0"))

# ── Throttle API ────────────────────────────────────────────────────────────────
THROTTLE_API_URL = os.environ.get("THROTTLE_API_URL", "")
THROTTLE_POLL_INTERVAL = float(os.environ.get("THROTTLE_POLL_INTERVAL_S", "10"))
EVENT_API_URL = os.environ.get("EVENT_API_URL", "")

_throttle_cache: dict = {"enabled": True, "rate_per_min": 0}
_throttle_last_fetch: float = 0.0


# ── Event API ───────────────────────────────────────────────────────────────────
def _post_delete_event(correl_hex: str, partition: int, offset: int,
                       deleted: int, depth: int = 0) -> None:
    """Fire-and-forget POST to the API's SSE publish endpoint."""
    if not EVENT_API_URL:
        return
    payload = json.dumps({
        "correl_id": correl_hex,
        "partition": partition,
        "offset": offset,
        "deleted": deleted,
        "depth": depth,
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }).encode()
    try:
        req = urllib.request.Request(
            EVENT_API_URL,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=3):
            pass
    except Exception as exc:
        log.debug("Could not post delete event to API: %s", exc)


# ── Throttle helpers ───────────────────────────────────────────────────────────
def fetch_throttle() -> dict:
    global _throttle_cache, _throttle_last_fetch
    if not THROTTLE_API_URL:
        return _throttle_cache
    now = time.monotonic()
    if now - _throttle_last_fetch < THROTTLE_POLL_INTERVAL:
        return _throttle_cache
    try:
        with urllib.request.urlopen(THROTTLE_API_URL, timeout=3) as resp:
            data = json.loads(resp.read().decode())
            _throttle_cache = data
            _throttle_last_fetch = now
            log.debug("Throttle config refreshed: %s", data)
    except Exception as exc:
        log.warning("Could not reach throttle API (%s) — using cached config: %s",
                    exc, _throttle_cache)
    return _throttle_cache


def apply_throttle(throttle: dict) -> None:
    if not throttle.get("enabled", True):
        log.info("Throttle: connector is PAUSED — sleeping until re-enabled")
        while True:
            time.sleep(THROTTLE_POLL_INTERVAL)
            current = fetch_throttle()
            if current.get("enabled", True):
                log.info("Throttle: connector RESUMED")
                break
        return
    rate = int(throttle.get("rate_per_min", 0))
    if rate > 0:
        delay = 60.0 / rate
        log.debug("Throttle: rate=%d/min — sleeping %.1fs", rate, delay)
        time.sleep(delay)


# ── MQ helpers ─────────────────────────────────────────────────────────────────
def connect_mq() -> pymqi.QueueManager:
    conn_info = f"{MQ_HOST}({MQ_PORT})"
    cd = pymqi.CD()
    cd.ChannelName = MQ_CHANNEL.encode()
    cd.ConnectionName = conn_info.encode()
    cd.ChannelType = pymqi.CMQC.MQCHT_CLNTCONN
    cd.TransportType = pymqi.CMQC.MQXPT_TCP

    qmgr = pymqi.QueueManager(None)
    if MQ_USER:
        qmgr.connect_with_options(
            MQ_QMGR,
            cd=cd,
            opts=pymqi.CMQC.MQCNO_CLIENT_BINDING,
            user=MQ_USER.encode(),
            password=MQ_PASSWORD.encode(),
        )
    else:
        qmgr.connect_with_options(MQ_QMGR, cd=cd, opts=pymqi.CMQC.MQCNO_CLIENT_BINDING)
    log.info("Connected to QMGR %s at %s", MQ_QMGR, conn_info)
    return qmgr


def delete_by_correl_id(
    qmgr: pymqi.QueueManager,
    target_correl_id: bytes,
    target_correl_hex: str,
) -> int:
    """
    Destructively GET the single message from MQ_QUEUE whose CorrelId matches
    target_correl_id exactly.

    Uses MQMO_MATCH_CORREL_ID so MQ does the lookup — no full queue scan needed.
    Returns 1 if deleted, 0 if not found.
    """
    log.info("Delete by CorrelId: %s on %s@%s", target_correl_hex, MQ_QUEUE, MQ_QMGR)

    md = pymqi.MD()
    md.CorrelId = target_correl_id

    gmo = pymqi.GMO()
    gmo.Options = pymqi.CMQC.MQGMO_NO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING
    gmo.MatchOptions = pymqi.CMQC.MQMO_MATCH_CORREL_ID

    q = pymqi.Queue(
        qmgr, MQ_QUEUE,
        pymqi.CMQC.MQOO_INPUT_EXCLUSIVE | pymqi.CMQC.MQOO_FAIL_IF_QUIESCING,
    )
    try:
        q.get(None, md, gmo)
        msg_id_hex = bytes(md.MsgId).hex()
        log.info("DELETED MsgId=%s CorrelId=%s", msg_id_hex, target_correl_hex)
        return 1
    except pymqi.MQMIError as e:
        if e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
            log.warning("CorrelId=%s not found on %s@%s — already gone or not yet arrived",
                        target_correl_hex, MQ_QUEUE, MQ_QMGR)
            return 0
        raise
    finally:
        q.close()


# ── Main loop ──────────────────────────────────────────────────────────────────
def build_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": 30000,
            "heartbeat.interval.ms": 10000,
        }
    )


def main():
    consumer = build_consumer()
    consumer.subscribe([KAFKA_TOPIC])
    log.info("Subscribed to %s on %s (group=%s)", KAFKA_TOPIC, KAFKA_BOOTSTRAP, KAFKA_GROUP_ID)

    try:
        while True:
            msg = consumer.poll(timeout=KAFKA_POLL_TIMEOUT)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            # ── Decode Kafka payload ──────────────────────────────────────────
            try:
                payload = json.loads(msg.value().decode())
                correl_hex: str = payload["correl_id"]
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                log.error("Bad Kafka message payload: %s — skipping", e)
                consumer.commit(message=msg)
                continue

            log.info(
                "Kafka event received — partition=%d offset=%d correl_id=%s",
                msg.partition(), msg.offset(), correl_hex,
            )

            target_bytes = bytes.fromhex(correl_hex)

            # ── Throttle check ────────────────────────────────────────────────
            apply_throttle(fetch_throttle())

            try:
                qmgr = connect_mq()
                try:
                    deleted = delete_by_correl_id(qmgr, target_bytes, correl_hex)
                finally:
                    qmgr.disconnect()
            except pymqi.MQMIError as e:
                log.error(
                    "MQ error while deleting CorrelId=%s: reason=%s — will retry on restart",
                    correl_hex, e.reason,
                )
                time.sleep(5)
                continue
            except Exception:
                log.exception("Unexpected error — will retry on restart")
                time.sleep(5)
                continue

            # Only commit the Kafka offset after MQ work succeeded
            consumer.commit(message=msg)
            log.info(
                "Kafka offset committed — partition=%d offset=%d",
                msg.partition(), msg.offset(),
            )

            _post_delete_event(
                correl_hex=correl_hex,
                partition=msg.partition(),
                offset=msg.offset(),
                deleted=deleted,
            )

    except KeyboardInterrupt:
        log.info("Shutting down")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
