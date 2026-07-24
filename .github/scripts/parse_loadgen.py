"""Turn one loadgen text report into a JSON line on stdout."""

import json
import re
import sys

UNIT_MS = {"us": 0.001, "ms": 1.0, "s": 1000.0}


def value_ms(token):
    match = re.fullmatch(r"([0-9.]+)(us|ms|s)", token)
    if not match:
        return None
    return round(float(match.group(1)) * UNIT_MS[match.group(2)], 4)


def parse_latency(line):
    fields = line.split()
    if fields[1] == "none":
        return None
    stats = {}
    for index in range(1, len(fields) - 1, 2):
        stats[fields[index].replace("p99.9", "p999")] = fields[index + 1]
    return {
        "count": int(stats["count"]),
        "p50_ms": value_ms(stats["p50"]),
        "p90_ms": value_ms(stats["p90"]),
        "p99_ms": value_ms(stats["p99"]),
        "p999_ms": value_ms(stats["p999"]),
        "max_ms": value_ms(stats["max"]),
    }


def main():
    workload, path = sys.argv[1], sys.argv[2]
    record = {"workload": workload}
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line.startswith("completed"):
                match = re.search(
                    r"\(([0-9]+) ops/s\), ([0-9]+) conflicts, ([0-9]+) failures", line
                )
                record["ops_per_s"] = int(match.group(1))
                record["conflicts"] = int(match.group(2))
                record["failures"] = int(match.group(3))
            elif line.startswith("reads"):
                record["reads"] = parse_latency(line)
            elif line.startswith("writes"):
                record["writes"] = parse_latency(line)
    if "ops_per_s" not in record:
        raise SystemExit(f"no completed line found in {path}")
    print(json.dumps(record))


main()
