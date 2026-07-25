"""Rewrite the published bench numbers from the newest main run.

Rewrites the marked section of README.md and the marked blocks of
web/index.html from the same result artifacts, so the site and the readme
can never disagree.

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
        passes = {}
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                record = json.loads(line)
                key = record.get("engine"), record["workload"]
                passes.setdefault(key, []).append(record)
        if passes:
            profiles[path.stem] = {
                key: sorted(runs, key=lambda run: run["ops_per_s"])[len(runs) // 2]
                for key, runs in passes.items()
            }
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


SITE = pathlib.Path("web/index.html")
FIELD_BEGIN = "<!-- bench:field:start -->"
FIELD_END = "<!-- bench:field:end -->"
MATRIX_BEGIN = "<!-- bench:matrix:start -->"
MATRIX_END = "<!-- bench:matrix:end -->"

ENGINE_NAMES = {
    "quantadb": "QuantaDB",
    "postgres18": "PostgreSQL 18",
    "mariadb114": "MariaDB 11.4",
    "mysql84": "MySQL 8.4",
}
WORKLOAD_LABELS = {"reads": "Reads", "mixed": "Mixed", "writes": "Writes"}

# A profile is called out when its writes fall under a quarter of the best
# profile's, or its write tail passes 20 ms. Both are visible on the page
# rather than dropped.
BAD_WRITE_SHARE = 0.25
BAD_P99_MS = 20.0


def value(records, workload, engine=None):
    record = records.get((engine, workload))
    return None if record is None else record["ops_per_s"]


def field_html(profiles, stamp, sha):
    records = profiles.get(FIELD_PROFILE)
    if not records:
        return None

    engines = [e for e in FIELD_ENGINES if any(key[0] == e for key in records)]
    if not engines:
        return None

    peak = {}
    for workload in WORKLOADS:
        found = [value(records, workload, e) for e in engines]
        found = [v for v in found if v is not None]
        peak[workload] = max(found) if found else None

    rows = [
        '  <div class="field">',
        '    <div class="field-row field-head" role="presentation">',
        '      <span class="field-name">Engine</span>',
        '      <span class="field-metric">Reads</span>',
        '      <span class="field-metric">Mixed 80/20</span>',
        '      <span class="field-metric">Writes</span>',
        "    </div>",
    ]

    for engine in engines:
        self_row = engine == "quantadb"
        rows.append("")
        rows.append(
            f'    <div class="field-row{" is-self" if self_row else ""}">'
        )
        rows.append(
            f'      <span class="field-name">{ENGINE_NAMES.get(engine, engine)}</span>'
        )
        for workload in WORKLOADS:
            found = value(records, workload, engine)
            best = peak[workload]
            label = WORKLOAD_LABELS[workload]
            if found is None or not best:
                rows.append(
                    f'      <span class="field-metric" data-label="{label}">'
                    f'<b>-</b></span>'
                )
                continue
            width = f"{found / best * 100:.1f}".rstrip("0").rstrip(".")
            leads = ""
            if found == best:
                leads = ' data-leads="yes"' if self_row else ' data-leads="rival"'
            rows.append(
                f'      <span class="field-metric" data-label="{label}"{leads}>'
            )
            rows.append(f'        <span class="bar" style="--w:{width}%"></span>')
            rows.append(f"        <b>{found:,}</b>")
            rows.append("      </span>")
        rows.append("    </div>")

    rows.append("  </div>")
    rows.extend(
        [
            "",
            '      <dl class="provenance">',
            f"        <div><dt>Measured</dt><dd>{stamp}</dd></div>",
            f"        <div><dt>Commit</dt><dd><code>{sha[:9]}</code></dd></div>",
            "        <div><dt>Limits</dt><dd>2 CPU, 2 GiB</dd></div>",
            "        <div><dt>Load</dt><dd>4 conn, 10 s</dd></div>",
            "      </dl>",
        ]
    )
    return "\n".join(rows)


def matrix_html(profiles, sha):
    if not profiles:
        return None

    ordered = sorted(
        profiles.items(),
        key=lambda item: value(item[1], "writes") or 0,
        reverse=True,
    )
    writes = [value(records, "writes") or 0 for _, records in ordered]
    best_writes = max(writes) if writes else 0

    rows = [
        '  <div class="matrix-scroll">',
        '    <table class="matrix">',
        "      <caption>QuantaDB on GitHub runners, measured on main at "
        f"<code>{sha[:9]}</code></caption>",
        "      <thead>",
        "        <tr>",
        '          <th scope="col">Profile</th>',
        '          <th scope="col">Reads</th>',
        '          <th scope="col">Mixed</th>',
        '          <th scope="col">Writes</th>',
        '          <th scope="col">Write p99</th>',
        "        </tr>",
        "      </thead>",
        "      <tbody>",
    ]

    for name, records in ordered:
        write_ops = value(records, "writes")
        tail = wp99(records, "writes")
        bad = False
        if write_ops is not None and best_writes:
            bad = write_ops < best_writes * BAD_WRITE_SHARE
        if tail != "-" and float(tail) > BAD_P99_MS:
            bad = True
        cells = []
        for workload in WORKLOADS:
            found = value(records, workload)
            cells.append("-" if found is None else f"{found:,}")
        marker = ' class="is-bad"' if bad else ""
        unit = "" if tail == "-" else " ms"
        rows.append(f"        <tr{marker}>")
        rows.append(f'          <th scope="row">{name}</th>')
        rows.append(
            f"          <td>{cells[0]}</td><td>{cells[1]}</td>"
            f"<td>{cells[2]}</td><td>{tail}{unit}</td>"
        )
        rows.append("        </tr>")

    rows.extend(["      </tbody>", "    </table>", "  </div>"])
    return "\n".join(rows)


def splice(text, begin, end, body):
    head, rest = text.split(begin, 1)
    middle, tail = rest.split(end, 1)
    trailing = middle.rsplit("\n", 1)[-1]
    indent = trailing if trailing.strip() == "" else ""
    return f"{head}{begin}\n{body}\n{indent}{end}{tail}"


def publish_site(results, reference, stamp, sha):
    if not SITE.exists():
        return
    text = SITE.read_text(encoding="utf-8")

    field = field_html(reference, stamp, sha)
    if field is not None:
        text = splice(text, FIELD_BEGIN, FIELD_END, field)

    matrix = matrix_html(results, sha)
    if matrix is not None:
        text = splice(text, MATRIX_BEGIN, MATRIX_END, matrix)

    SITE.write_text(text, encoding="utf-8")


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

    publish_site(results, reference, stamp, sha)


main()
