"""
browse_connector/main.py
────────────────────────
Every BROWSE_INTERVAL_SECONDS (default 5) this connector:
  1. Destructively GETs all available messages from ORDERS.INBOUND on QMGRA
  2. For each consumed message, produces a Kafka message to KAFKA_TOPIC with:
       key   = CorrelId hex string
       value = JSON envelope containing queue name, qmgr, correl_id, msg_id
  3. Flushes and sleeps until next cycle

The delete connector on QMGRB consumes these events and deletes the matching
message from QMGRB by CorrelId.

Environment variables (see config/browse_connector.env):
  MQ_HOST, MQ_PORT, MQ_QMGR, MQ_QUEUE, MQ_CHANNEL, MQ_USER, MQ_PASSWORD
  KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, BROWSE_INTERVAL_SECONDS
"""

import json
import logging
import os
import time

import pymqi
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [browse-connector] %(levelname)s %(message)s",
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
BROWSE_INTERVAL = int(os.environ.get("BROWSE_INTERVAL_SECONDS", "5"))


# ── Kafka helpers ──────────────────────────────────────────────────────────────
def delivery_report(err, msg):
    if err:
        log.error("Kafka delivery failed: %s", err)
    else:
        log.debug(
            "Produced to %s [%d] offset %d key=%s",
            msg.topic(), msg.partition(), msg.offset(), msg.key().decode(),
        )


def build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
            "retry.backoff.ms": 500,
        }
    )


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


def consume_and_publish(qmgr: pymqi.QueueManager, producer: Producer) -> int:
    """
    Destructively GET all messages from MQ_QUEUE.
    For each message, publish a Kafka event with its CorrelId.
    Returns the number of messages consumed.
    """
    gmo = pymqi.GMO()
    gmo.Options = pymqi.CMQC.MQGMO_NO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING
    gmo.MatchOptions = pymqi.CMQC.MQMO_NONE

    q = pymqi.Queue(
        qmgr, MQ_QUEUE,
        pymqi.CMQC.MQOO_INPUT_EXCLUSIVE | pymqi.CMQC.MQOO_FAIL_IF_QUIESCING,
    )
    count = 0
    try:
        while True:
            md = pymqi.MD()
            try:
                q.get(None, md, gmo)
            except pymqi.MQMIError as e:
                if e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                    break
                raise

            correl_hex = bytes(md.CorrelId).hex()
            msg_id_hex = bytes(md.MsgId).hex()
            count += 1

            payload = json.dumps(
                {
                    "queue": MQ_QUEUE,
                    "qmgr": MQ_QMGR,
                    "correl_id": correl_hex,
                    "msg_id": msg_id_hex,
                    "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }
            ).encode()

            producer.produce(
                topic=KAFKA_TOPIC,
                key=correl_hex.encode(),
                value=payload,
                callback=delivery_report,
            )
            log.info("Consumed and published: correl_id=%s msg_id=%s", correl_hex, msg_id_hex)
    finally:
        q.close()

    if count > 0:
        producer.flush(timeout=30)
        log.info("Cycle complete — consumed and published %d message(s) from %s", count, MQ_QUEUE)
    else:
        log.debug("Queue %s is empty — nothing to consume", MQ_QUEUE)

    return count


# ── Main loop ──────────────────────────────────────────────────────────────────
def main():
    producer = build_producer()

    while True:
        cycle_start = time.monotonic()
        try:
            qmgr = connect_mq()
            try:
                consume_and_publish(qmgr, producer)
            finally:
                qmgr.disconnect()
        except pymqi.MQMIError as e:
            log.error("MQ error during consume cycle: %s (reason %s)", e, e.reason)
        except Exception:
            log.exception("Unexpected error in consume cycle")

        elapsed = time.monotonic() - cycle_start
        sleep_for = max(0, BROWSE_INTERVAL - elapsed)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
