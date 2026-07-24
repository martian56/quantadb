"""Rewrite the README bench section from the newest main run.

Usage: readme_bench.py <results_dir> <reference_dir> <reference_stamp> <sha>
"""

import json
import pathlib
import sys

BEGIN = "<!-- bench:start -->"
END = "<!-- bench:end -->"
WORKLOADS = ["reads", "mixed", "writes"]
FIELD_PROFILE = "linux-x64-2cpu-2gb"
FIELD_ENGINES = ["quantadb", "postgres18", "mariadb114", "mysql84"]


def load_profiles(directory):
    profiles = {}
    for path in sorted(pathlib.Path(directory).rglob("*.jsonl")):
        records = {}
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                record = json.loads(line)
                key = record.get("engine"), record["workload"]
                records[key] = record
        if records:
            profiles[path.stem] = records
    return profiles


def ops(records, workload, engine=None):
    record = records.get((engine, workload))
    return "-" if record is None else str(record["ops_per_s"])


def wp99(records, workload, engine=None):
    record = records.get((engine, workload))
    value = None if record is None else record.get("writes", record).get("p99_ms")
    if value is None and record is not None and "write_p99_ms" in record:
        value = record["write_p99_ms"]
    return "-" if value is None else f"{value:.1f}"


def matrix_table(profiles):
    lines = [
        "QuantaDB on GitHub runners, loadgen native protocol, 10 seconds per",
        "workload at 4 connections:",
        "",
        "| profile | reads ops/s | mixed ops/s | writes ops/s | write p99 ms |",
        "|---|---:|---:|---:|---:|",
    ]
    for name, records in profiles.items():
        cells = [ops(records, workload) for workload in WORKLOADS]
        lines.append(
            f"| {name} | {cells[0]} | {cells[1]} | {cells[2]} "
            f"| {wp99(records, 'writes')} |"
        )
    return lines


def field_table(profiles, stamp):
    records = profiles.get(FIELD_PROFILE)
    if not records:
        return []
    lines = [
        "",
        f"The field at 2 CPUs and 2 GiB, identical containers, one shared",
        f"driver through each engine's Postgres or MySQL port, measured {stamp}:",
        "",
        "| engine | reads ops/s | mixed ops/s | writes ops/s |",
        "|---|---:|---:|---:|",
    ]
    for engine in FIELD_ENGINES:
        if not any(key[0] == engine for key in records):
            continue
        label = f"**{engine}**" if engine == "quantadb" else engine
        lines.append(
            f"| {label} | {ops(records, 'reads', engine)} "
            f"| {ops(records, 'mixed', engine)} "
            f"| {ops(records, 'writes', engine)} |"
        )
    lines.append("")
    lines.append(
        "MySQL and MariaDB read throughput is limited by the pure Python"
        " client in the shared driver; their write numbers are server bound."
    )
    return lines


def main():
    results_dir, reference_dir, stamp, sha = (
        sys.argv[1],
        sys.argv[2],
        sys.argv[3],
        sys.argv[4],
    )
    results = load_profiles(results_dir)
    if not results:
        raise SystemExit("no results to publish")
    reference = load_profiles(reference_dir) if reference_dir else {}

    section = [BEGIN, f"Measured on main at `{sha[:9]}`.", ""]
    section.extend(matrix_table(results))
    section.extend(field_table(reference, stamp))
    section.append(END)

    readme = pathlib.Path("README.md")
    text = readme.read_text(encoding="utf-8")
    head, rest = text.split(BEGIN, 1)
    _, tail = rest.split(END, 1)
    readme.write_text(head + "\n".join(section) + tail, encoding="utf-8")


main()
