"""Build the bench comparison comment from result and baseline records.

Usage: bench_report.py <results_dir> <baseline_dir> <output_md> [baseline_sha]
"""

import json
import pathlib
import sys

MARKER = "<!-- quantadb-bench-report -->"
NOISE_PERCENT = 10.0
WORKLOADS = ["reads", "mixed", "writes"]


def load(directory):
    profiles = {}
    root = pathlib.Path(directory)
    if not root.is_dir():
        return profiles
    for path in sorted(root.rglob("*.jsonl")):
        records = {}
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                record = json.loads(line)
                records[record["workload"]] = record
        if records:
            profiles[path.stem] = records
    return profiles


def fmt_ms(value):
    return "-" if value is None else f"{value:.2f}"


def ops_cell(base, current):
    if base is None:
        return f"{current}"
    return f"{base} → {current}"


def delta_cell(base, current, higher_is_better=True):
    if base in (None, 0) or current is None:
        return "new"
    change = (current - base) / base * 100.0
    signal = change if higher_is_better else -change
    if signal >= NOISE_PERCENT:
        mark = "🟢"
    elif signal <= -NOISE_PERCENT:
        mark = "🔴"
    else:
        mark = "⚪"
    return f"{mark} {change:+.1f}%"


def latency_cell(base_record, record, side, key):
    def pick(item):
        if item is None:
            return None
        stats = item.get(side)
        return None if stats is None else stats.get(key)

    now = pick(record)
    if now is None:
        return "-"
    base = pick(base_record)
    if base is None:
        return fmt_ms(now)
    return f"{fmt_ms(base)} → {fmt_ms(now)}"


def profile_section(name, records, base_records):
    lines = [
        f"### {name}",
        "",
        "| workload | ops/s | Δ ops/s | read p99 ms | write p99 ms | write p99.9 ms |",
        "|---|---|---|---|---|---|",
    ]
    for workload in WORKLOADS:
        record = records.get(workload)
        if record is None:
            continue
        base_record = base_records.get(workload) if base_records else None
        base_ops = None if base_record is None else base_record.get("ops_per_s")
        lines.append(
            "| {} | {} | {} | {} | {} | {} |".format(
                workload,
                ops_cell(base_ops, record["ops_per_s"]),
                delta_cell(base_ops, record["ops_per_s"]),
                latency_cell(base_record, record, "reads", "p99_ms"),
                latency_cell(base_record, record, "writes", "p99_ms"),
                latency_cell(base_record, record, "writes", "p999_ms"),
            )
        )
    lines.append("")
    return lines


def overall_verdict(profiles, baselines):
    deltas = []
    for name, records in profiles.items():
        base_records = baselines.get(name, {})
        for workload, record in records.items():
            base = base_records.get(workload)
            if base and base.get("ops_per_s"):
                change = (
                    (record["ops_per_s"] - base["ops_per_s"])
                    / base["ops_per_s"]
                    * 100.0
                )
                deltas.append((change, f"{name} {workload}"))
    if not deltas:
        return "No baseline from main yet; this run becomes the reference."
    worst = min(deltas)
    best = max(deltas)
    if worst[0] <= -NOISE_PERCENT:
        return f"⚠️ Worst regression: {worst[1]} at {worst[0]:+.1f}%."
    if best[0] >= NOISE_PERCENT:
        return f"✅ Best improvement: {best[1]} at {best[0]:+.1f}%. Nothing regressed beyond noise."
    return "✅ All profiles within the ±10% noise band of main."


def main():
    results_dir, baseline_dir, output = sys.argv[1], sys.argv[2], sys.argv[3]
    baseline_sha = sys.argv[4] if len(sys.argv) > 4 else ""
    profiles = load(results_dir)
    baselines = load(baseline_dir)

    lines = [MARKER, "## Bench report", ""]
    lines.append(overall_verdict(profiles, baselines))
    lines.append("")
    if baselines:
        reference = f"main @ {baseline_sha[:9]}" if baseline_sha else "main"
        lines.append(
            f"Baseline: {reference}. Cells read baseline → this PR. "
            f"Verdicts need more than ±{NOISE_PERCENT:.0f}% to leave the noise band."
        )
    else:
        lines.append("Baseline: none found on main yet, values shown without deltas.")
    lines.append("")
    for name, records in profiles.items():
        lines.extend(profile_section(name, records, baselines.get(name, {})))
    lines.append(
        "_Shared CI runners: treat single-profile swings as suggestive, "
        "agreement across profiles as real._"
    )
    pathlib.Path(output).write_text("\n".join(lines) + "\n", encoding="utf-8")


main()
