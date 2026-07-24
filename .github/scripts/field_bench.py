"""One workload driver for every engine in the field reference.

Usage: field_bench.py <engine_label> <driver> <port> <output_jsonl>
Drivers: postgres, mysql, quantadb. QuantaDB is measured through its
Postgres port with the same psycopg2 driver real Postgres gets.
"""

import json
import random
import sys
import threading
import time

ROWS = 10_000
WORKERS = 8
SECONDS = 15
SEED_BATCH = 500

WORKLOADS = [
    ("reads", 1.00),
    ("mixed", 0.80),
    ("writes", 0.00),
]

CONFLICT_MARKERS = ("40001", "1213", "Deadlock", "write conflict", "range conflict")


def connect(driver, port):
    if driver == "mysql":
        import pymysql

        return pymysql.connect(
            host="127.0.0.1",
            port=port,
            user="root",
            password="bench",
            database="bench",
            autocommit=True,
        )
    import psycopg2

    connection = psycopg2.connect(
        host="127.0.0.1",
        port=port,
        user="postgres" if driver == "postgres" else "quanta",
        password="bench" if driver == "postgres" else None,
        dbname="postgres" if driver == "postgres" else "quanta",
    )
    connection.autocommit = True
    return connection


def setup(driver, port):
    connection = connect(driver, port)
    cursor = connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS bench_kv")
    cursor.execute("CREATE TABLE bench_kv (id BIGINT PRIMARY KEY, val TEXT NOT NULL)")
    row = 0
    while row < ROWS:
        end = min(row + SEED_BATCH, ROWS)
        values = ", ".join(f"({n}, 'seed {n}')" for n in range(row, end))
        cursor.execute(f"INSERT INTO bench_kv (id, val) VALUES {values}")
        row = end
    cursor.close()
    connection.close()


def is_conflict(error):
    text = str(error)
    code = getattr(error, "pgcode", None)
    return code == "40001" or any(marker in text for marker in CONFLICT_MARKERS)


def worker(driver, port, read_share, deadline, out, index):
    rng = random.Random(0x5EED + index)
    connection = connect(driver, port)
    cursor = connection.cursor()
    reads, writes = [], []
    conflicts = 0
    while time.perf_counter() < deadline:
        key = rng.randrange(ROWS)
        is_read = rng.random() < read_share
        if is_read:
            sql = f"SELECT val FROM bench_kv WHERE id = {key}"
        else:
            sql = f"UPDATE bench_kv SET val = 'w{rng.getrandbits(32)}' WHERE id = {key}"
        started = time.perf_counter()
        while True:
            try:
                cursor.execute(sql)
                if is_read:
                    cursor.fetchall()
                break
            except Exception as error:
                if is_conflict(error):
                    conflicts += 1
                    continue
                raise
        elapsed = time.perf_counter() - started
        (reads if is_read else writes).append(elapsed)
    cursor.close()
    connection.close()
    out[index] = (reads, writes, conflicts)


def percentile_ms(values, quantile):
    if not values:
        return None
    ranked = sorted(values)
    position = min(len(ranked) - 1, round((len(ranked) - 1) * quantile))
    return round(ranked[position] * 1000, 4)


def run_workload(engine, driver, port, label, read_share):
    deadline = time.perf_counter() + SECONDS
    out = [None] * WORKERS
    threads = [
        threading.Thread(target=worker, args=(driver, port, read_share, deadline, out, i))
        for i in range(WORKERS)
    ]
    started = time.perf_counter()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    elapsed = time.perf_counter() - started

    for slot in out:
        if slot is None:
            raise RuntimeError("a worker thread died; see the traceback above")
    reads = [value for slot in out for value in slot[0]]
    writes = [value for slot in out for value in slot[1]]
    conflicts = sum(slot[2] for slot in out)
    record = {
        "engine": engine,
        "workload": label,
        "ops_per_s": round((len(reads) + len(writes)) / elapsed),
        "read_p50_ms": percentile_ms(reads, 0.50),
        "read_p99_ms": percentile_ms(reads, 0.99),
        "write_p50_ms": percentile_ms(writes, 0.50),
        "write_p99_ms": percentile_ms(writes, 0.99),
        "write_p999_ms": percentile_ms(writes, 0.999),
        "conflict_retries": conflicts,
    }
    print(f"  {engine} {label}: {record['ops_per_s']} ops/s", flush=True)
    return record


def main():
    if sys.argv[1] == "--probe":
        try:
            connect(sys.argv[2], int(sys.argv[3])).close()
        except Exception:
            raise SystemExit(1)
        return
    engine, driver, port, output = (
        sys.argv[1],
        sys.argv[2],
        int(sys.argv[3]),
        sys.argv[4],
    )
    setup(driver, port)
    with open(output, "a", encoding="utf-8") as handle:
        for label, read_share in WORKLOADS:
            record = run_workload(engine, driver, port, label, read_share)
            handle.write(json.dumps(record) + "\n")


main()
