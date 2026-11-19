from datetime import date, datetime, timedelta
import uuid

from cassandra import InvalidRequest
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import FallthroughRetryPolicy, WhiteListRoundRobinPolicy

profile = ExecutionProfile(
    load_balancing_policy=WhiteListRoundRobinPolicy(["127.0.0.1"]),
    retry_policy=FallthroughRetryPolicy(),
)
cluster = Cluster(["127.0.0.1"], execution_profiles={EXEC_PROFILE_DEFAULT: profile})
session = cluster.connect()


def setup_exercises_keyspace():
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS lab_ex
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)


# ============================================================
# Exercise 1:
#   1) "latest 10 orders of a customer"
#   2) "orders per day"
# ============================================================
def exercise_1_orders():
    print("--- Exercise 1: Orders Tables ---")
    session.execute("""
        CREATE TABLE IF NOT EXISTS lab_ex.orders_by_customer (
            customer_id text,
            order_ts    timestamp,
            order_id    uuid,
            total       double,
            PRIMARY KEY ((customer_id), order_ts, order_id)
        ) WITH CLUSTERING ORDER BY (order_ts DESC, order_id ASC)
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS lab_ex.orders_by_day (
            order_day   date,
            order_ts    timestamp,
            order_id    uuid,
            customer_id text,
            total       double,
            PRIMARY KEY ((order_day), order_ts, order_id)
        ) WITH CLUSTERING ORDER BY (order_ts DESC, order_id ASC)
    """)

    ins_cust = session.prepare(
        "INSERT INTO lab_ex.orders_by_customer (customer_id, order_ts, order_id, total) VALUES (?, ?, ?, ?)"
    )
    ins_day = session.prepare(
        "INSERT INTO lab_ex.orders_by_day (order_day, order_ts, order_id, customer_id, total) VALUES (?, ?, ?, ?, ?)"
    )

    base = datetime(2026, 11, 1, 8, 0)
    for i in range(12):
        ts = base + timedelta(hours=i)
        oid = uuid.uuid4()
        total = round(100.0 + i * 15.5, 2)
        session.execute(ins_cust, ("C01", ts, oid, total))
        session.execute(ins_day, (ts.date(), ts, oid, "C01", total))

    latest_10 = session.execute(
        "SELECT customer_id, order_ts, order_id, total FROM lab_ex.orders_by_customer WHERE customer_id = %s LIMIT 10",
        ("C01",),
    ).all()
    print(f"Latest 10 orders for C01: {len(latest_10)} rows (newest ts = {latest_10[0].order_ts})")

    day_orders = session.execute(
        "SELECT order_day, order_ts, customer_id, total FROM lab_ex.orders_by_day WHERE order_day = %s",
        (date(2026, 11, 1),),
    ).all()
    print(f"Orders on 2026-11-01: {len(day_orders)} rows")


# ============================================================
# Exercise 2:
#   Insert readings for 3 sensors and try a query that omits
#   the partition key. Read the error.
# ============================================================
def exercise_2_omit_partition_key():
    print("\n--- Exercise 2: Query Without Partition Key ---")
    session.execute("""
        CREATE TABLE IF NOT EXISTS lab_ex.readings_by_sensor (
            sensor_id   text,
            ts          timestamp,
            temperature double,
            humidity    double,
            PRIMARY KEY ((sensor_id), ts)
        ) WITH CLUSTERING ORDER BY (ts DESC)
    """)

    ins = session.prepare(
        "INSERT INTO lab_ex.readings_by_sensor (sensor_id, ts, temperature, humidity) VALUES (?, ?, ?, ?)"
    )
    now = datetime(2026, 11, 1, 10, 0)
    for idx, sid in enumerate(["s-01", "s-02", "s-03"]):
        session.execute(ins, (sid, now + timedelta(minutes=idx), 28.5 + idx * 2, 70.0))

    try:
        session.execute("SELECT * FROM lab_ex.readings_by_sensor WHERE temperature > 30.0")
    except InvalidRequest as e:
        print("Caught expected InvalidRequest:", e)


# ============================================================
# Exercise 3:
#   Monthly partition bucket: PRIMARY KEY ((sensor_id, month), ts)
# ============================================================
def exercise_3_monthly_bucket():
    print("\n--- Exercise 3: Monthly Partition Bucket ---")
    session.execute("""
        CREATE TABLE IF NOT EXISTS lab_ex.readings_by_sensor_month (
            sensor_id   text,
            month       text,
            ts          timestamp,
            temperature double,
            humidity    double,
            PRIMARY KEY ((sensor_id, month), ts)
        ) WITH CLUSTERING ORDER BY (ts DESC)
    """)

    ins = session.prepare(
        "INSERT INTO lab_ex.readings_by_sensor_month (sensor_id, month, ts, temperature, humidity) "
        "VALUES (?, ?, ?, ?, ?)"
    )
    samples = [
        ("hn-01", "2026-10", datetime(2026, 10, 31, 23, 55), 26.4, 78.0),
        ("hn-01", "2026-11", datetime(2026, 11, 1, 0, 0), 25.9, 80.0),
        ("hn-01", "2026-11", datetime(2026, 11, 1, 0, 5), 26.1, 79.5),
    ]
    for row in samples:
        session.execute(ins, row)

    nov_rows = session.execute(
        "SELECT sensor_id, month, ts, temperature FROM lab_ex.readings_by_sensor_month "
        "WHERE sensor_id = %s AND month = %s",
        ("hn-01", "2026-11"),
    ).all()
    print(f"Readings for hn-01 in 2026-11: {len(nov_rows)} rows")
    print(
        "Giải thích: Gom nhóm partition theo (sensor_id, month) giúp giới hạn kích thước mỗi partition "
        "theo tháng, tránh partition phình to vô hạn (unbounded partition) khi sensor ghi dữ liệu nhiều năm."
    )


if __name__ == "__main__":
    try:
        setup_exercises_keyspace()
        exercise_1_orders()
        exercise_2_omit_partition_key()
        exercise_3_monthly_bucket()
    finally:
        cluster.shutdown()
