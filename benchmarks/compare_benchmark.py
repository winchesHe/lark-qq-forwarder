#!/usr/bin/env python3
"""比较两个 benchmark_forwarder.py 输出，给出是否满足正向优化门槛。"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="比较转发基准结果")
    parser.add_argument("baseline", type=Path)
    parser.add_argument("optimized", type=Path)
    args = parser.parse_args()
    baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
    optimized = json.loads(args.optimized.read_text(encoding="utf-8"))
    b = baseline["metrics"]
    o = optimized["metrics"]

    def change(name: str) -> float:
        old, new = float(b[name]), float(o[name])
        return (new - old) / old * 100 if old else 0.0

    latency_change = change("elapsed_ms_p95")
    throughput_change = change("throughput_messages_per_second_median")
    downloads_old = int(b["image_download_count_per_run"])
    downloads_new = int(o["image_download_count_per_run"])
    report = {
        "schema_version": 1,
        "baseline": str(args.baseline),
        "optimized": str(args.optimized),
        "changes_percent": {
            "elapsed_ms_p95": round(latency_change, 2),
            "throughput_messages_per_second_median": round(throughput_change, 2),
        },
        "image_download_count": {"baseline": downloads_old, "optimized": downloads_new},
        "positive_optimization": (
            latency_change <= -20.0 or throughput_change >= 25.0
        )
        and downloads_new <= downloads_old
        and int(o["sent_count_per_run"]) == int(b["sent_count_per_run"]),
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
