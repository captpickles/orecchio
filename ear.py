
#!/usr/bin/env python3

import argparse
import csv
import datetime as dt
import json
import re
import signal
import subprocess
import sys
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd
import tensorflow as tf
import tensorflow_hub as hub

YAMNET_HANDLE = "https://tfhub.dev/google/yamnet/1"
SAMPLE_RATE = 16000
BYTES_PER_SAMPLE = 2  # pcm_s16le


def load_dotenv(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    env: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        env[key] = value
    return env


def env_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso(ts: dt.datetime) -> str:
    return ts.isoformat(timespec="seconds")


def utc_day(ts: dt.datetime) -> str:
    return ts.strftime("%Y-%m-%d")


@dataclass
class EventRule:
    name: str
    threshold: float
    quiet_gap_seconds: float
    min_duration_seconds: float
    max_duration_seconds: float | None = None
    explicit_labels: set[str] = field(default_factory=set)
    contains_terms: tuple[str, ...] = field(default_factory=tuple)


@dataclass
class ActiveEvent:
    event_type: str
    start: dt.datetime
    last_seen: dt.datetime
    peak_score: float
    matched_labels: set[str] = field(default_factory=set)
    hit_count: int = 1


class EventTracker:
    def __init__(self, rules: dict[str, EventRule], chunk_seconds: float):
        self.rules = rules
        self.chunk_seconds = chunk_seconds
        self.active: dict[str, ActiveEvent] = {}

    def _duration_seconds(self, event: ActiveEvent) -> float:
        # Include the currently observed chunk so single-hit events are not 0s long.
        return (event.last_seen - event.start).total_seconds() + self.chunk_seconds

    def update(self, event_type: str, label: str, score: float, ts: dt.datetime) -> None:
        current = self.active.get(event_type)
        if current is None:
            self.active[event_type] = ActiveEvent(
                event_type=event_type,
                start=ts,
                last_seen=ts,
                peak_score=score,
                matched_labels={label},
                hit_count=1,
            )
            return

        current.last_seen = ts
        current.peak_score = max(current.peak_score, score)
        current.matched_labels.add(label)
        current.hit_count += 1

    def flush_finished(self, ts: dt.datetime) -> list[dict]:
        finished = []

        for event_type, current in list(self.active.items()):
            rule = self.rules[event_type]
            silence = (ts - current.last_seen).total_seconds()
            if silence < rule.quiet_gap_seconds:
                continue

            duration = self._duration_seconds(current)

            keep = duration >= rule.min_duration_seconds
            if rule.max_duration_seconds is not None:
                keep = keep and duration <= rule.max_duration_seconds

            if keep:
                finished.append({
                    "event_type": current.event_type,
                    "start": current.start,
                    "end": current.last_seen,
                    "duration_seconds": duration,
                    "peak_score": current.peak_score,
                    "matched_labels": sorted(current.matched_labels),
                    "hit_count": current.hit_count,
                })

            del self.active[event_type]

        return finished

    def flush_all(self) -> list[dict]:
        finished = []
        for event_type, current in list(self.active.items()):
            rule = self.rules[event_type]
            duration = self._duration_seconds(current)

            keep = duration >= rule.min_duration_seconds
            if rule.max_duration_seconds is not None:
                keep = keep and duration <= rule.max_duration_seconds

            if keep:
                finished.append({
                    "event_type": current.event_type,
                    "start": current.start,
                    "end": current.last_seen,
                    "duration_seconds": duration,
                    "peak_score": current.peak_score,
                    "matched_labels": sorted(current.matched_labels),
                    "hit_count": current.hit_count,
                })

            del self.active[event_type]

        return finished


def load_yamnet():
    model = hub.load(YAMNET_HANDLE)
    class_map_path = model.class_map_path().numpy().decode("utf-8")
    class_names = list(pd.read_csv(class_map_path)["display_name"])
    return model, class_names


def start_ffmpeg(rtsp_url: str) -> subprocess.Popen:
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",
        "-rtsp_transport", "tcp",
        "-i", rtsp_url,
        "-vn",
        "-ac", "1",
        "-ar", str(SAMPLE_RATE),
        "-f", "s16le",
        "-acodec", "pcm_s16le",
        "-",
    ]
    return subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )


def pcm16_to_float(raw: bytes) -> np.ndarray:
    arr = np.frombuffer(raw, dtype=np.int16).astype(np.float32)
    if arr.size == 0:
        return np.array([], dtype=np.float32)
    return arr / 32768.0


def score_clip(model, class_names, waveform: np.ndarray, top_k: int = 10) -> list[tuple[str, float]]:
    scores, _, _ = model(waveform)
    mean_scores = tf.reduce_mean(scores, axis=0).numpy()
    idx = np.argsort(mean_scores)[::-1][:top_k]
    return [(class_names[i], float(mean_scores[i])) for i in idx]


def build_rules() -> dict[str, EventRule]:
    return {
        "siren": EventRule(
            name="siren",
            threshold=0.22,
            quiet_gap_seconds=4.0,
            min_duration_seconds=6.0,
            max_duration_seconds=45.0,
            explicit_labels={
                "police car (siren)",
                "ambulance (siren)",
            },
            contains_terms=("siren",),
        ),
        "mower": EventRule(
            name="mower",
            threshold=0.08,
            quiet_gap_seconds=20.0,
            min_duration_seconds=30.0,
            explicit_labels={"lawn mower"},
            contains_terms=("heavy engine", "medium engine", "idling"),
        ),
        "bell": EventRule(
            name="bell",
            threshold=0.10,
            quiet_gap_seconds=10.0,
            min_duration_seconds=2.0,
            explicit_labels={"church bell", "tubular bells"},
            contains_terms=("church bell", "tubular bells", "campanology", "change ringing"),
        ),
        "revving": EventRule(
            name="revving",
            threshold=0.12,
            quiet_gap_seconds=3.0,
            min_duration_seconds=2.0,
            max_duration_seconds=20.0,
            explicit_labels={"accelerating, revving, vroom"},
            contains_terms=("revving", "accelerating", "vroom"),
        ),
        "engine_idle": EventRule(
            name="engine_idle",
            threshold=0.25,
            quiet_gap_seconds=20.0,
            min_duration_seconds=15.0,
            contains_terms=("idling", "engine", "truck", "bus"),
        ),
        "train_horn": EventRule(
            name="train_horn",
            threshold=0.35,
            quiet_gap_seconds=10.0,
            min_duration_seconds=1.0,
            max_duration_seconds=30.0,
            explicit_labels={"train horn", "train whistle"},
            contains_terms=("train horn", "train whistle"),
        ),
        # Optional, still fuzzy:
        # "blower": EventRule(
        #     name="blower",
        #     threshold=0.25,
        #     quiet_gap_seconds=10.0,
        #     min_duration_seconds=10.0,
        #     contains_terms=("blower", "power tool", "vacuum cleaner"),
        # ),
    }


def classify(top_labels: list[tuple[str, float]], rules: dict[str, EventRule]) -> list[tuple[str, str, float]]:
    best_by_event: dict[str, tuple[str, float]] = {}
    for label, score in top_labels:
        label_l = label.lower()

        for event_type, rule in rules.items():
            if score < rule.threshold:
                continue

            explicit_labels = {v.lower() for v in rule.explicit_labels}
            contains_match = any(re.search(rf"\b{re.escape(term)}\b", label_l) for term in rule.contains_terms)
            matched = label_l in explicit_labels or contains_match
            if not matched:
                continue

            current = best_by_event.get(event_type)
            if current is None or score > current[1]:
                best_by_event[event_type] = (label, score)

    return [(event_type, label, score) for event_type, (label, score) in best_by_event.items()]


def load_daily_summary(path: Path):
    summary = defaultdict(lambda: defaultdict(lambda: {
        "event_count": 0,
        "total_duration_seconds": 0.0,
    }))

    if not path.exists():
        return summary

    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            d = row["date_utc"]
            e = row["event_type"]
            summary[d][e]["event_count"] = int(row["event_count"])
            summary[d][e]["total_duration_seconds"] = float(row["total_duration_seconds"])

    return summary


def write_daily_summary(path: Path, summary) -> None:
    rows = []
    for day in sorted(summary.keys()):
        for event_type in sorted(summary[day].keys()):
            stats = summary[day][event_type]
            count = stats["event_count"]
            total = stats["total_duration_seconds"]
            avg = total / count if count else 0.0
            rows.append({
                "date_utc": day,
                "event_type": event_type,
                "event_count": count,
                "total_duration_seconds": round(total, 2),
                "avg_duration_seconds": round(avg, 2),
            })

    with path.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "date_utc",
                "event_type",
                "event_count",
                "total_duration_seconds",
                "avg_duration_seconds",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def append_event(path: Path, event: dict) -> None:
    write_header = not path.exists()

    with path.open("a", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow([
                "timestamp_start_utc",
                "timestamp_end_utc",
                "duration_seconds",
                "event_type",
                "peak_score",
                "hit_count",
                "matched_labels",
            ])

        writer.writerow([
            iso(event["start"]),
            iso(event["end"]),
            f'{event["duration_seconds"]:.2f}',
            event["event_type"],
            f'{event["peak_score"]:.4f}',
            event["hit_count"],
            "; ".join(event["matched_labels"]),
        ])


def ensure_events_csv(path: Path) -> None:
    if path.exists() and path.stat().st_size > 0:
        return
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp_start_utc",
            "timestamp_end_utc",
            "duration_seconds",
            "event_type",
            "peak_score",
            "hit_count",
            "matched_labels",
        ])


def notify_webhook(url: str, event: dict) -> None:
    payload = {
        "event_type": event["event_type"],
        "start": iso(event["start"]),
        "end": iso(event["end"]),
        "duration_seconds": round(event["duration_seconds"], 2),
        "peak_score": round(event["peak_score"], 4),
        "matched_labels": event["matched_labels"],
        "hit_count": event["hit_count"],
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            resp.read()
    except Exception as exc:
        print(f"[{iso(utc_now())}] webhook failed: {exc}", file=sys.stderr)


def notify_slack(url: str, event: dict, daily_count: int) -> None:
    event_name = event["event_type"].replace("_", " ").title()
    start_local = event["start"].astimezone()
    clock_time = start_local.strftime("%I:%M %p").lstrip("0")
    duration_s = int(round(event["duration_seconds"]))
    summary = f"{event_name}: {duration_s}s at {clock_time}. {daily_count} total today."

    payload = {
        "text": summary,
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            resp.read()
    except Exception as exc:
        print(f"[{iso(utc_now())}] slack webhook failed: {exc}", file=sys.stderr)


def parse_firebase_conf(path: Path) -> str:
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8", errors="ignore")

    m = re.search(r"realtime\s*db\s*url\s*:\s*(https?://\S+)", text, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip().rstrip("/")

    m = re.search(r'databaseURL"\s*:\s*"([^"]+)"', text, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip().rstrip("/")

    return ""


def firebase_url(base_url: str, path: str, auth_token: str) -> str:
    clean_base = base_url.rstrip("/")
    parts = [urllib.parse.quote(part, safe="") for part in path.strip("/").split("/") if part]
    full = f"{clean_base}/{'/'.join(parts)}.json"
    if auth_token:
        full = f"{full}?auth={urllib.parse.quote(auth_token, safe='')}"
    return full


def firebase_request(base_url: str, path: str, payload: dict, method: str, auth_token: str = "") -> None:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        firebase_url(base_url, path, auth_token),
        data=data,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        resp.read()


def notify_firebase(base_url: str, auth_token: str, event: dict, day_count: int, day_total_duration: float) -> None:
    event_payload = {
        "event_type": event["event_type"],
        "start_utc": iso(event["start"]),
        "end_utc": iso(event["end"]),
        "duration_seconds": round(float(event["duration_seconds"]), 2),
        "peak_score": round(float(event["peak_score"]), 4),
        "hit_count": int(event["hit_count"]),
        "matched_labels": list(event["matched_labels"]),
        "date_utc": utc_day(event["start"]),
        "event_count_for_day": int(day_count),
        "total_duration_seconds_for_day": round(float(day_total_duration), 2),
        "ingested_at_utc": iso(utc_now()),
    }
    firebase_request(base_url, "events", event_payload, method="POST", auth_token=auth_token)

    daily_payload = {
        "date_utc": utc_day(event["start"]),
        "event_type": event["event_type"],
        "event_count": int(day_count),
        "total_duration_seconds": round(float(day_total_duration), 2),
        "avg_duration_seconds": round(float(day_total_duration) / day_count, 2) if day_count else 0.0,
        "updated_at_utc": iso(utc_now()),
    }
    firebase_request(
        base_url,
        f"daily_summary/{utc_day(event['start'])}/{event['event_type']}",
        daily_payload,
        method="PUT",
        auth_token=auth_token,
    )


def main():
    env = load_dotenv(Path(".env"))

    parser = argparse.ArgumentParser(description="RTSP audio nuisance-event logger using YAMNet")
    parser.add_argument("--rtsp", default=env.get("ORECCHIO_RTSP", ""), help="RTSP URL")
    parser.add_argument("--events-csv", default=env.get("ORECCHIO_EVENTS_CSV", "events.csv"))
    parser.add_argument("--daily-csv", default=env.get("ORECCHIO_DAILY_CSV", "daily_summary.csv"))
    parser.add_argument("--chunk-seconds", type=float, default=float(env.get("ORECCHIO_CHUNK_SECONDS", "5.0")))
    parser.set_defaults(dump_top=env_bool(env.get("ORECCHIO_DUMP_TOP"), default=False))
    parser.add_argument("--dump-top", dest="dump_top", action="store_true")
    parser.add_argument("--no-dump-top", dest="dump_top", action="store_false")
    parser.add_argument("--notify-webhook", default=env.get("ORECCHIO_NOTIFY_WEBHOOK", ""), help="Optional webhook URL")
    parser.add_argument(
        "--slack-webhook",
        default=env.get("ORECCHIO_SLACK_WEBHOOK", ""),
        help="Optional Slack Incoming Webhook URL",
    )
    parser.add_argument(
        "--firebase-conf",
        default=env.get("ORECCHIO_FIREBASE_CONF", ""),
        help="Optional config file containing Realtime DB URL",
    )
    parser.add_argument(
        "--firebase-db-url",
        default=env.get("ORECCHIO_FIREBASE_DB_URL", ""),
        help="Firebase Realtime Database URL",
    )
    parser.add_argument(
        "--firebase-auth-token",
        default=env.get("ORECCHIO_FIREBASE_AUTH_TOKEN", ""),
        help="Optional Firebase RTDB auth token/secret",
    )

    args = parser.parse_args()
    if not args.rtsp:
        raise ValueError("RTSP URL required. Set --rtsp or ORECCHIO_RTSP in .env")
    if args.chunk_seconds <= 0:
        raise ValueError("--chunk-seconds must be > 0")

    chunk_samples = int(SAMPLE_RATE * args.chunk_seconds)
    chunk_bytes = chunk_samples * BYTES_PER_SAMPLE

    rules = build_rules()
    tracker = EventTracker(rules, chunk_seconds=args.chunk_seconds)

    model, class_names = load_yamnet()
    ffmpeg = start_ffmpeg(args.rtsp)
    events_csv = Path(args.events_csv)
    daily_csv = Path(args.daily_csv)
    ensure_events_csv(events_csv)
    daily_summary = load_daily_summary(daily_csv)
    write_daily_summary(daily_csv, daily_summary)

    firebase_db_url = args.firebase_db_url.strip().rstrip("/")
    if not firebase_db_url:
        conf_path = Path(args.firebase_conf) if args.firebase_conf else Path("firebase.conf")
        firebase_db_url = parse_firebase_conf(conf_path)
    firebase_enabled = bool(firebase_db_url)
    if firebase_enabled:
        print(f"[{iso(utc_now())}] firebase enabled: {firebase_db_url}")

    if ffmpeg.stdout is None:
        raise RuntimeError("ffmpeg stdout unavailable")

    def persist(events: list[dict]) -> None:
        for event in events:
            append_event(events_csv, event)

            day = utc_day(event["start"])
            daily_summary[day][event["event_type"]]["event_count"] += 1
            daily_summary[day][event["event_type"]]["total_duration_seconds"] += event["duration_seconds"]
            write_daily_summary(daily_csv, daily_summary)

            print(
                f"[{iso(utc_now())}] closed "
                f"{event['event_type']} "
                f"duration={event['duration_seconds']:.1f}s "
                f"peak={event['peak_score']:.3f} "
                f"hits={event['hit_count']} "
                f"labels={event['matched_labels']}"
            )

            if args.notify_webhook:
                notify_webhook(args.notify_webhook, event)
            if args.slack_webhook:
                today_count = daily_summary[day][event["event_type"]]["event_count"]
                notify_slack(args.slack_webhook, event, today_count)
            if firebase_enabled:
                try:
                    stats = daily_summary[day][event["event_type"]]
                    notify_firebase(
                        firebase_db_url,
                        args.firebase_auth_token,
                        event,
                        day_count=stats["event_count"],
                        day_total_duration=stats["total_duration_seconds"],
                    )
                except Exception as exc:
                    print(f"[{iso(utc_now())}] firebase write failed: {exc}", file=sys.stderr)

    def shutdown(*_):
        try:
            persist(tracker.flush_all())
        finally:
            try:
                ffmpeg.terminate()
            except Exception:
                pass
            sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print(f"[{iso(utc_now())}] listening...")

    pending = bytearray()

    while True:
        if len(pending) < chunk_bytes:
            raw = ffmpeg.stdout.read(chunk_bytes - len(pending))
            if not raw:
                persist(tracker.flush_all())
                err = ""
                if ffmpeg.stderr is not None:
                    try:
                        err = ffmpeg.stderr.read().decode("utf-8", errors="ignore")
                    except Exception:
                        pass
                raise RuntimeError(f"ffmpeg stream ended. stderr:\n{err}")
            pending.extend(raw)

        while len(pending) >= chunk_bytes:
            chunk_raw = bytes(pending[:chunk_bytes])
            del pending[:chunk_bytes]
            ts = utc_now()

            waveform = pcm16_to_float(chunk_raw)
            if waveform.size == 0:
                persist(tracker.flush_finished(ts))
                continue

            top = score_clip(model, class_names, waveform, top_k=20)

            if args.dump_top:
                pretty = ", ".join(f"{label}={score:.3f}" for label, score in top)
                print(f"[{iso(ts)}] top: {pretty}")

            matches = classify(top, rules)
            for event_type, matched_label, score in matches:
                tracker.update(event_type, matched_label, score, ts)

            persist(tracker.flush_finished(ts))


if __name__ == "__main__":
    main()
