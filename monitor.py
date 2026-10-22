
import time
import requests
import oracledb
import psycopg2

from confluent_kafka import Consumer, TopicPartition
from prometheus_client import start_http_server, Gauge


# --- Prometheus metrics ---
g_src = Gauge("pipeline_oracle_rows", "Rows in Oracle source table")
g_tgt = Gauge("pipeline_pg_rows", "Rows in Postgres target table")
g_diff = Gauge("pipeline_row_diff", "Oracle rows minus Postgres rows")
g_lag = Gauge("pipeline_sink_lag", "Estimated sink consumer lag in events")
g_rate = Gauge("pipeline_events_per_sec", "Events per second into Kafka topic")
g_src_up = Gauge("pipeline_source_running", "1 if oracle-source and its tasks are RUNNING")
g_sink_up = Gauge("pipeline_sink_running", "1 if jdbc-sink and its tasks are RUNNING")

# Exposes metrics at http://localhost:8000/metrics
start_http_server(8000)


# --- Configuration ---
CONNECT = "http://localhost:8083"
TOPIC = "src5.BANKUSER.BANK_TRANSACTIONS"
SINK_GROUP = "connect-jdbc-sink"
KAFKA_BOOTSTRAP = "localhost:29092"

# --- Kafka consumer for monitoring offsets ---
kc = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": SINK_GROUP,
    "enable.auto.commit": False,
})

# --- Database connections ---
ora = oracledb.connect(
    user="bankuser",
    password="bankpass",
    dsn="localhost:1521/XEPDB1",
)

pg = psycopg2.connect(
    "dbname=target user=target password=target host=localhost port=5432"
)
pg.autocommit = True


def connector_status(name):
    """Return a readable status and whether connector/tasks are all RUNNING."""
    try:
        response = requests.get(
            f"{CONNECT}/connectors/{name}/status",
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()

        connector_state = data["connector"]["state"]
        tasks = data.get("tasks", [])
        task_states = [task["state"] for task in tasks]

        task_text = ",".join(task_states) if task_states else "NO_TASKS"
        label = f"{connector_state}/{task_text}"

        healthy = (
            connector_state == "RUNNING"
            and bool(task_states)
            and all(state == "RUNNING" for state in task_states)
        )

        return label, healthy

    except Exception as exc:
        return f"UNREACHABLE({exc})", False


def kafka_stats():
    """Return total topic end offsets and estimated consumer-group lag."""
    metadata = kc.list_topics(TOPIC, timeout=10)
    topic_metadata = metadata.topics.get(TOPIC)

    if topic_metadata is None or topic_metadata.error is not None:
        raise RuntimeError(f"Kafka topic not found or unavailable: {TOPIC}")

    partitions = [
        TopicPartition(TOPIC, partition_id)
        for partition_id in topic_metadata.partitions
    ]

    committed = kc.committed(partitions, timeout=10)

    total_end_offset = 0
    total_lag = 0

    for tp in committed:
        low, high = kc.get_watermark_offsets(
            TopicPartition(TOPIC, tp.partition),
            timeout=10,
        )

        total_end_offset += high
        current = tp.offset if tp.offset >= 0 else low
        total_lag += max(high - current, 0)

    return total_end_offset, total_lag


def count(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]


print("Starting monitor... (Ctrl+C to stop)")
print("Prometheus metrics: http://localhost:8000/metrics\n")

previous_end = None
previous_time = None

while True:
    try:
        # Kafka offsets and event-rate estimate
        end_offset, lag = kafka_stats()
        now = time.time()

        rate = (
            max(end_offset - previous_end, 0) / (now - previous_time)
            if previous_end is not None and now > previous_time
            else 0.0
        )

        previous_end = end_offset
        previous_time = now

        # Database row counts
        src = count(
            ora,
            "SELECT COUNT(*) FROM bankuser.bank_transactions",
        )
        tgt = count(
            pg,
            "SELECT COUNT(*) FROM bank_transactions",
        )

        # Connector health: fetch each status once per cycle
        source_label, source_healthy = connector_status("oracle-source")
        sink_label, sink_healthy = connector_status("jdbc-sink")

        # Update Prometheus metrics
        g_src.set(src)
        g_tgt.set(tgt)
        g_diff.set(src - tgt)
        g_lag.set(lag)
        g_rate.set(rate)
        g_src_up.set(1 if source_healthy else 0)
        g_sink_up.set(1 if sink_healthy else 0)

        # Console output
        print(
            f"{time.strftime('%H:%M:%S')} | "
            f"src={source_label:<25} "
            f"sink={sink_label:<25} | "
            f"events/s={rate:7.1f} | "
            f"sink_lag={lag:6d} | "
            f"oracle_rows={src:8,d} "
            f"pg_rows={tgt:8,d} "
            f"diff={src - tgt:+6d}"
        )

    except Exception as exc:
        print(f"{time.strftime('%H:%M:%S')} | monitor error: {exc}")

    time.sleep(10)
