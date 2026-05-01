#!/usr/bin/env python3

import argparse
import csv
import datetime as dt
from collections import defaultdict
from pathlib import Path


def parse_iso(ts: str) -> dt.datetime:
    ts = ts.strip()
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return dt.datetime.fromisoformat(ts)


def load_events(path: Path, start: dt.datetime | None, end: dt.datetime | None) -> list[dict]:
    if not path.exists():
        return []
    out: list[dict] = []
    with path.open("r", newline="", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                start_ts = parse_iso(row["timestamp_start_utc"])
                end_ts = parse_iso(row["timestamp_end_utc"])
                duration = float(row["duration_seconds"])
            except Exception:
                continue
            if start and start_ts < start:
                continue
            if end and end_ts > end:
                continue
            out.append(
                {
                    "start": start_ts,
                    "end": end_ts,
                    "event_type": row["event_type"].strip(),
                    "duration_seconds": duration,
                }
            )
    return out


def summarize(events: list[dict]) -> dict[str, dict[str, float]]:
    stats: dict[str, dict[str, float]] = defaultdict(lambda: {"count": 0.0, "total_duration": 0.0})
    for e in events:
        key = e["event_type"]
        stats[key]["count"] += 1.0
        stats[key]["total_duration"] += e["duration_seconds"]
    return stats


def nearest_match_delta_seconds(target: dict, candidates: list[dict], tolerance_s: float) -> float | None:
    best: float | None = None
    for c in candidates:
        if c["event_type"] != target["event_type"]:
            continue
        delta = abs((c["start"] - target["start"]).total_seconds())
        if delta > tolerance_s:
            continue
        if best is None or delta < best:
            best = delta
    return best


def compare_alignment(legacy: list[dict], orecchio: list[dict], tolerance_s: float) -> tuple[int, int]:
    matched = 0
    for event in legacy:
        if nearest_match_delta_seconds(event, orecchio, tolerance_s) is not None:
            matched += 1
    unmatched = max(0, len(legacy) - matched)
    return matched, unmatched


def print_summary(name: str, stats: dict[str, dict[str, float]], total: int) -> None:
    print(f"{name}: events={total}")
    for event_type in sorted(stats.keys()):
        count = int(stats[event_type]["count"])
        total_dur = stats[event_type]["total_duration"]
        avg = total_dur / count if count else 0.0
        print(f"  - {event_type}: count={count} total_duration_s={total_dur:.1f} avg_duration_s={avg:.1f}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare ear.py vs orecchio.py event outputs in shadow mode.")
    parser.add_argument("--legacy-events", default="events.csv", help="Path to legacy ear.py events CSV")
    parser.add_argument("--orecchio-events", default="orecchio_events.csv", help="Path to orecchio events CSV")
    parser.add_argument("--start-utc", default="", help="Optional ISO start bound (inclusive)")
    parser.add_argument("--end-utc", default="", help="Optional ISO end bound (inclusive)")
    parser.add_argument(
        "--start-tolerance-seconds",
        type=float,
        default=30.0,
        help="Tolerance for start-time alignment checks",
    )
    args = parser.parse_args()

    start = parse_iso(args.start_utc) if args.start_utc else None
    end = parse_iso(args.end_utc) if args.end_utc else None

    legacy = load_events(Path(args.legacy_events), start, end)
    orecchio = load_events(Path(args.orecchio_events), start, end)

    legacy_stats = summarize(legacy)
    orecchio_stats = summarize(orecchio)
    print_summary("legacy", legacy_stats, len(legacy))
    print_summary("orecchio", orecchio_stats, len(orecchio))

    matched, unmatched = compare_alignment(legacy, orecchio, args.start_tolerance_seconds)
    print(
        "alignment: "
        f"matched={matched} unmatched_legacy={unmatched} "
        f"tolerance_s={args.start_tolerance_seconds:.1f}"
    )


if __name__ == "__main__":
    main()
