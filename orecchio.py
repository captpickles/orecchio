#!/usr/bin/env python3

import argparse
import csv
import datetime as dt
import getpass
import io
import json
import os
import re
import signal
import subprocess
import sys
import time
import tomllib
import urllib.parse
import urllib.request
from contextlib import redirect_stderr, redirect_stdout
from collections import defaultdict, deque
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any

import numpy as np
try:
    import firebase_admin
    from firebase_admin import credentials as firebase_credentials
    from firebase_admin import db as firebase_db
except Exception:
    firebase_admin = None
    firebase_credentials = None
    firebase_db = None

BYTES_PER_SAMPLE = 2  # pcm_s16le


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso(ts: dt.datetime) -> str:
    return ts.isoformat(timespec="seconds")


def configure_stdio() -> None:
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream is None:
            continue
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(line_buffering=True, write_through=True)


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


def env_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def parse_csv_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in override.items():
        if key in out and isinstance(out[key], dict) and isinstance(value, dict):
            out[key] = deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def nested_set(target: dict[str, Any], path: tuple[str, ...], value: Any) -> None:
    current = target
    for part in path[:-1]:
        current = current.setdefault(part, {})
    current[path[-1]] = value


def nested_get(target: dict[str, Any], path: tuple[str, ...], default: Any = None) -> Any:
    current: Any = target
    for part in path:
        if not isinstance(current, dict) or part not in current:
            return default
        current = current[part]
    return current


def default_config() -> dict[str, Any]:
    return {
        "site": {"id": "", "timezone": "UTC"},
        "source": {
            "provider": "avfoundation",
            "device": "",
            "rtsp_url": "",
            "sample_rate_hz": 48000,
            "channels": 1,
            "ffmpeg_bin": "ffmpeg",
            "reconnect_delay_seconds": 3.0,
        },
        "runtime": {
            "ring_buffer_seconds": 900,
            "drop_policy": "drop_oldest_branch_job",
            "dump_top": False,
            "read_chunk_seconds": 0.5,
            "health_report_seconds": 30.0,
        },
        "branches": {
            "enabled": ["yamnet", "birdnet"],
            "yamnet": {
                "enabled": True,
                "sample_rate_hz": 16000,
                "window_seconds": 5.0,
                "hop_seconds": 5.0,
                "top_k": 20,
            },
            "birdnet": {
                "enabled": True,
                "sample_rate_hz": 48000,
                "window_seconds": 3.0,
                "hop_seconds": 1.5,
                "min_confidence": 0.25,
                "log_zero_detections": False,
                "lat": None,
                "lon": None,
                "date_mode": "window_start_utc",
                "ebird_taxonomy_csv": "ebird_taxonomy.csv",
            },
        },
        "outputs": {
            "local": {
                "events_csv": "orecchio_events.csv",
                "daily_csv": "orecchio_daily_summary.csv",
                "detections_jsonl": "orecchio_detections.jsonl",
            },
            "firebase": {
                "enabled": False,
                "db_url": "",
                "auth_token": "",
                "api_key": "",
                "refresh_token": "",
                "id_token": "",
                "service_account": "",
                "base_path": "orecchio_sites",
            },
            "birdweather": {
                "enabled": False,
                "station_id": "",
                "api_token": "",
                "require_manual_review": True,
                "queue_jsonl": "orecchio_birdweather_queue.jsonl",
                "sent_jsonl": "orecchio_birdweather_sent.jsonl",
            },
        },
    }


def load_toml_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("rb") as f:
        loaded = tomllib.load(f)
    if not isinstance(loaded, dict):
        raise ValueError(f"Invalid TOML root in {path}")
    return loaded


def apply_env_overrides(cfg: dict[str, Any], env: dict[str, str]) -> dict[str, Any]:
    override: dict[str, Any] = {}
    mappings: list[tuple[str, tuple[str, ...], str]] = [
        ("ORECCHIO_SITE_ID", ("site", "id"), "str"),
        ("ORECCHIO_SITE_TIMEZONE", ("site", "timezone"), "str"),
        ("ORECCHIO_SOURCE_PROVIDER", ("source", "provider"), "str"),
        ("ORECCHIO_SOURCE_DEVICE", ("source", "device"), "str"),
        ("ORECCHIO_SOURCE_RTSP_URL", ("source", "rtsp_url"), "str"),
        ("ORECCHIO_SOURCE_SAMPLE_RATE_HZ", ("source", "sample_rate_hz"), "int"),
        ("ORECCHIO_SOURCE_CHANNELS", ("source", "channels"), "int"),
        ("ORECCHIO_FFMPEG_BIN", ("source", "ffmpeg_bin"), "str"),
        ("ORECCHIO_RECONNECT_DELAY_SECONDS", ("source", "reconnect_delay_seconds"), "float"),
        ("ORECCHIO_RING_BUFFER_SECONDS", ("runtime", "ring_buffer_seconds"), "int"),
        ("ORECCHIO_DROP_POLICY", ("runtime", "drop_policy"), "str"),
        ("ORECCHIO_DUMP_TOP", ("runtime", "dump_top"), "bool"),
        ("ORECCHIO_HEALTH_REPORT_SECONDS", ("runtime", "health_report_seconds"), "float"),
        ("ORECCHIO_ENABLE_BRANCHES", ("branches", "enabled"), "csv"),
        ("ORECCHIO_FIREBASE_ENABLED", ("outputs", "firebase", "enabled"), "bool"),
        ("ORECCHIO_FIREBASE_DB_URL", ("outputs", "firebase", "db_url"), "str"),
        ("ORECCHIO_FIREBASE_AUTH_TOKEN", ("outputs", "firebase", "auth_token"), "str"),
        ("ORECCHIO_FIREBASE_API_KEY", ("outputs", "firebase", "api_key"), "str"),
        ("ORECCHIO_FIREBASE_REFRESH_TOKEN", ("outputs", "firebase", "refresh_token"), "str"),
        ("ORECCHIO_FIREBASE_ID_TOKEN", ("outputs", "firebase", "id_token"), "str"),
        ("ORECCHIO_FIREBASE_SERVICE_ACCOUNT", ("outputs", "firebase", "service_account"), "str"),
        ("ORECCHIO_FIREBASE_BASE_PATH", ("outputs", "firebase", "base_path"), "str"),
        ("ORECCHIO_BIRDWEATHER_ENABLED", ("outputs", "birdweather", "enabled"), "bool"),
        ("ORECCHIO_BIRDWEATHER_STATION_ID", ("outputs", "birdweather", "station_id"), "str"),
        ("ORECCHIO_BIRDWEATHER_API_TOKEN", ("outputs", "birdweather", "api_token"), "str"),
        ("ORECCHIO_BIRDWEATHER_REQUIRE_MANUAL_REVIEW", ("outputs", "birdweather", "require_manual_review"), "bool"),
        ("ORECCHIO_BIRDWEATHER_QUEUE_JSONL", ("outputs", "birdweather", "queue_jsonl"), "str"),
        ("ORECCHIO_BIRDWEATHER_SENT_JSONL", ("outputs", "birdweather", "sent_jsonl"), "str"),
        ("ORECCHIO_BIRDNET_TAXONOMY_CSV", ("branches", "birdnet", "ebird_taxonomy_csv"), "str"),
    ]
    for env_key, path, kind in mappings:
        raw = env.get(env_key)
        if raw is None:
            continue
        if kind == "int":
            value: Any = int(raw)
        elif kind == "float":
            value = float(raw)
        elif kind == "bool":
            value = env_bool(raw)
        elif kind == "csv":
            value = parse_csv_list(raw)
        else:
            value = raw
        nested_set(override, path, value)
    return deep_merge(cfg, override)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="orecchio audio platform scaffold")
    parser.add_argument("command", nargs="?", choices=["run", "login"], default="run")
    parser.add_argument("--config", default="orecchio.toml", help="Path to TOML config file")
    parser.add_argument("--site-id", default=None)
    parser.add_argument("--source-provider", default=None, choices=["avfoundation", "rtsp"])
    parser.add_argument("--source-device", default=None)
    parser.add_argument("--source-rtsp-url", default=None)
    parser.add_argument("--enable-branches", default=None, help="Comma-separated list, e.g. yamnet,birdnet")
    parser.add_argument("--dump-top", action="store_true", default=False)
    parser.add_argument("--no-dump-top", action="store_true", default=False)
    parser.add_argument("--firebase-api-key", default=None, help="Firebase Web API key (login command)")
    parser.add_argument("--firebase-email", default=None, help="Firebase Auth user email (login command)")
    parser.add_argument("--firebase-password", default=None, help="Firebase Auth user password (login command)")
    return parser


def apply_cli_overrides(cfg: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    override: dict[str, Any] = {}
    if args.site_id:
        nested_set(override, ("site", "id"), args.site_id)
    if args.source_provider:
        nested_set(override, ("source", "provider"), args.source_provider)
    if args.source_device:
        nested_set(override, ("source", "device"), args.source_device)
    if args.source_rtsp_url:
        nested_set(override, ("source", "rtsp_url"), args.source_rtsp_url)
    if args.enable_branches:
        nested_set(override, ("branches", "enabled"), parse_csv_list(args.enable_branches))
    if args.dump_top:
        nested_set(override, ("runtime", "dump_top"), True)
    if args.no_dump_top:
        nested_set(override, ("runtime", "dump_top"), False)
    return deep_merge(cfg, override)


def validate_config(cfg: dict[str, Any]) -> None:
    site_id = nested_get(cfg, ("site", "id"), "")
    if not site_id:
        raise ValueError("site.id is required (set in orecchio.toml, env, or --site-id)")

    provider = nested_get(cfg, ("source", "provider"))
    if provider not in {"avfoundation", "rtsp"}:
        raise ValueError("source.provider must be one of: avfoundation, rtsp")

    if provider == "avfoundation" and not nested_get(cfg, ("source", "device"), ""):
        raise ValueError("source.device is required when source.provider=avfoundation")
    if provider == "rtsp" and not nested_get(cfg, ("source", "rtsp_url"), ""):
        raise ValueError("source.rtsp_url is required when source.provider=rtsp")

    sample_rate = int(nested_get(cfg, ("source", "sample_rate_hz"), 0))
    channels = int(nested_get(cfg, ("source", "channels"), 0))
    if sample_rate <= 0:
        raise ValueError("source.sample_rate_hz must be > 0")
    if channels != 1:
        raise ValueError("source.channels must be 1 for now")

    ring_seconds = int(nested_get(cfg, ("runtime", "ring_buffer_seconds"), 0))
    if ring_seconds <= 0:
        raise ValueError("runtime.ring_buffer_seconds must be > 0")

    drop_policy = str(nested_get(cfg, ("runtime", "drop_policy"), ""))
    if drop_policy not in {"drop_oldest_branch_job", "drop_newest_branch_job"}:
        raise ValueError("runtime.drop_policy must be drop_oldest_branch_job or drop_newest_branch_job")

    health_report_seconds = float(nested_get(cfg, ("runtime", "health_report_seconds"), 0.0))
    if health_report_seconds <= 0:
        raise ValueError("runtime.health_report_seconds must be > 0")

    firebase_enabled = bool(nested_get(cfg, ("outputs", "firebase", "enabled"), False))
    if firebase_enabled:
        db_url = str(nested_get(cfg, ("outputs", "firebase", "db_url"), "")).strip()
        service_account = str(nested_get(cfg, ("outputs", "firebase", "service_account"), "")).strip()
        auth_token = str(nested_get(cfg, ("outputs", "firebase", "auth_token"), "")).strip()
        api_key = str(nested_get(cfg, ("outputs", "firebase", "api_key"), "")).strip()
        refresh_token = str(nested_get(cfg, ("outputs", "firebase", "refresh_token"), "")).strip()
        if not db_url and not service_account:
            raise ValueError(
                "outputs.firebase.enabled=true requires outputs.firebase.db_url or outputs.firebase.service_account"
            )
        if not service_account and not auth_token and not (api_key and refresh_token):
            raise ValueError(
                "firebase REST mode requires outputs.firebase.auth_token or "
                "outputs.firebase.api_key + outputs.firebase.refresh_token"
            )
        if not str(nested_get(cfg, ("outputs", "firebase", "base_path"), "")).strip():
            raise ValueError("outputs.firebase.base_path must be non-empty when firebase is enabled")

    birdweather_enabled = bool(nested_get(cfg, ("outputs", "birdweather", "enabled"), False))
    if birdweather_enabled:
        queue_jsonl = str(nested_get(cfg, ("outputs", "birdweather", "queue_jsonl"), "")).strip()
        sent_jsonl = str(nested_get(cfg, ("outputs", "birdweather", "sent_jsonl"), "")).strip()
        if not queue_jsonl:
            raise ValueError("outputs.birdweather.queue_jsonl must be non-empty when birdweather is enabled")
        if not sent_jsonl:
            raise ValueError("outputs.birdweather.sent_jsonl must be non-empty when birdweather is enabled")

    enabled = nested_get(cfg, ("branches", "enabled"), [])
    if isinstance(enabled, str):
        enabled = parse_csv_list(enabled)
    if not isinstance(enabled, list):
        raise ValueError("branches.enabled must be a list or comma-separated string")
    for name in enabled:
        section = nested_get(cfg, ("branches", str(name)), {})
        if not isinstance(section, dict) or not bool(section.get("enabled", False)):
            continue
        branch_sr = int(section.get("sample_rate_hz", 0))
        branch_window = float(section.get("window_seconds", 0.0))
        branch_hop = float(section.get("hop_seconds", 0.0))
        if branch_sr <= 0:
            raise ValueError(f"branches.{name}.sample_rate_hz must be > 0")
        if branch_window <= 0:
            raise ValueError(f"branches.{name}.window_seconds must be > 0")
        if branch_hop <= 0:
            raise ValueError(f"branches.{name}.hop_seconds must be > 0")
        if str(name) == "birdnet":
            min_conf = float(section.get("min_confidence", 0.0))
            if min_conf < 0.0 or min_conf > 1.0:
                raise ValueError("branches.birdnet.min_confidence must be in [0.0, 1.0]")
            lat = section.get("lat")
            lon = section.get("lon")
            if lat is not None and not (-90.0 <= float(lat) <= 90.0):
                raise ValueError("branches.birdnet.lat must be within [-90, 90]")
            if lon is not None and not (-180.0 <= float(lon) <= 180.0):
                raise ValueError("branches.birdnet.lon must be within [-180, 180]")
            date_mode = str(section.get("date_mode", "window_start_utc"))
            if date_mode not in {"window_start_utc", "today_utc", "disabled"}:
                raise ValueError("branches.birdnet.date_mode must be one of: window_start_utc, today_utc, disabled")


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


class RollingRatioDetector:
    def __init__(
        self,
        *,
        label_name: str,
        hit_threshold: float,
        window_seconds: float,
        min_ratio_to_open: float,
        min_ratio_to_stay_open: float,
        chunk_seconds: float,
    ):
        self.label_name = label_name.lower()
        self.hit_threshold = hit_threshold
        self.window_seconds = window_seconds
        self.min_ratio_to_open = min_ratio_to_open
        self.min_ratio_to_stay_open = min_ratio_to_stay_open
        self.chunk_seconds = chunk_seconds
        self.samples: deque[tuple[dt.datetime, bool, float]] = deque()
        self.active = False

    def _trim(self, ts: dt.datetime) -> None:
        cutoff = ts - dt.timedelta(seconds=self.window_seconds)
        while self.samples and self.samples[0][0] < cutoff:
            self.samples.popleft()

    def _has_full_window(self) -> bool:
        if len(self.samples) < 2:
            return False
        span = (self.samples[-1][0] - self.samples[0][0]).total_seconds() + self.chunk_seconds
        return span >= self.window_seconds

    def observe(self, ts: dt.datetime, top_labels: list[tuple[str, float]]) -> tuple[bool, float]:
        score_by_label = {label.lower(): score for label, score in top_labels}
        score = score_by_label.get(self.label_name, 0.0)
        is_hit = score >= self.hit_threshold
        self.samples.append((ts, is_hit, score))
        self._trim(ts)

        if self.samples and self._has_full_window():
            hit_count = sum(1 for _, hit, _ in self.samples if hit)
            ratio = hit_count / len(self.samples)
            if not self.active and ratio >= self.min_ratio_to_open:
                self.active = True
            elif self.active and ratio < self.min_ratio_to_stay_open:
                self.active = False
        return self.active, score


class EventTracker:
    def __init__(self, rules: dict[str, EventRule], chunk_seconds: float):
        self.rules = rules
        self.chunk_seconds = chunk_seconds
        self.active: dict[str, ActiveEvent] = {}

    def _duration_seconds(self, event: ActiveEvent) -> float:
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

    def _close(self, event_type: str, current: ActiveEvent) -> dict:
        return {
            "event_type": current.event_type,
            "start": current.start,
            "end": current.last_seen,
            "duration_seconds": self._duration_seconds(current),
            "peak_score": current.peak_score,
            "matched_labels": sorted(current.matched_labels),
            "hit_count": current.hit_count,
        }

    def flush_finished(self, ts: dt.datetime) -> list[dict]:
        finished: list[dict] = []
        for event_type, current in list(self.active.items()):
            rule = self.rules[event_type]
            silence = (ts - current.last_seen).total_seconds()
            if silence < rule.quiet_gap_seconds:
                continue
            event = self._close(event_type, current)
            keep = event["duration_seconds"] >= rule.min_duration_seconds
            if rule.max_duration_seconds is not None:
                keep = keep and event["duration_seconds"] <= rule.max_duration_seconds
            if keep:
                finished.append(event)
            del self.active[event_type]
        return finished

    def flush_all(self) -> list[dict]:
        finished: list[dict] = []
        for event_type, current in list(self.active.items()):
            rule = self.rules[event_type]
            event = self._close(event_type, current)
            keep = event["duration_seconds"] >= rule.min_duration_seconds
            if rule.max_duration_seconds is not None:
                keep = keep and event["duration_seconds"] <= rule.max_duration_seconds
            if keep:
                finished.append(event)
            del self.active[event_type]
        return finished


def build_yamnet_rules() -> dict[str, EventRule]:
    return {
        "siren": EventRule(
            name="siren",
            threshold=0.22,
            quiet_gap_seconds=4.0,
            min_duration_seconds=6.0,
            max_duration_seconds=45.0,
            explicit_labels={"police car (siren)", "ambulance (siren)"},
            contains_terms=("siren",),
        ),
        "mower": EventRule(
            name="mower",
            threshold=0.08,
            quiet_gap_seconds=20.0,
            min_duration_seconds=360.0,
            explicit_labels={"lawn mower"},
            contains_terms=("heavy engine", "medium engine", "idling"),
        ),
        "bell": EventRule(
            name="bell",
            threshold=0.25,
            quiet_gap_seconds=10.0,
            min_duration_seconds=4.0,
            explicit_labels={"tubular bells"},
            contains_terms=("tubular bells", "bell", "campanology", "change ringing"),
        ),
        "revving": EventRule(
            name="revving",
            threshold=0.20,
            quiet_gap_seconds=3.0,
            min_duration_seconds=4.0,
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
            min_duration_seconds=5.0,
            max_duration_seconds=30.0,
            explicit_labels={"train horn", "train whistle"},
            contains_terms=("train horn", "train whistle"),
        ),
    }


def build_yamnet_regimes(chunk_seconds: float) -> dict[str, RollingRatioDetector]:
    return {
        "mower": RollingRatioDetector(
            label_name="vehicle",
            hit_threshold=0.20,
            window_seconds=600.0,
            min_ratio_to_open=0.80,
            min_ratio_to_stay_open=0.70,
            chunk_seconds=chunk_seconds,
        )
    }


def classify_yamnet(top_labels: list[tuple[str, float]], rules: dict[str, EventRule]) -> list[tuple[str, str, float]]:
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


def load_yamnet_model():
    import pandas as pd
    import tensorflow as tf
    import tensorflow_hub as hub

    model = hub.load("https://tfhub.dev/google/yamnet/1")
    class_map_path = model.class_map_path().numpy().decode("utf-8")
    class_names = list(pd.read_csv(class_map_path)["display_name"])
    return model, class_names, tf


def score_yamnet_clip(model, class_names, tf, waveform: np.ndarray, top_k: int) -> list[tuple[str, float]]:
    scores, _, _ = model(waveform)
    mean_scores = tf.reduce_mean(scores, axis=0).numpy()
    idx = np.argsort(mean_scores)[::-1][:top_k]
    return [(class_names[i], float(mean_scores[i])) for i in idx]


def ensure_events_csv(path: Path) -> None:
    if path.exists() and path.stat().st_size > 0:
        return
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "timestamp_start_utc",
                "timestamp_end_utc",
                "duration_seconds",
                "event_type",
                "peak_score",
                "hit_count",
                "matched_labels",
            ]
        )


def append_event(path: Path, event: dict) -> None:
    write_header = not path.exists()
    with path.open("a", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(
                [
                    "timestamp_start_utc",
                    "timestamp_end_utc",
                    "duration_seconds",
                    "event_type",
                    "peak_score",
                    "hit_count",
                    "matched_labels",
                ]
            )
        writer.writerow(
            [
                iso(event["start"]),
                iso(event["end"]),
                f'{event["duration_seconds"]:.2f}',
                event["event_type"],
                f'{event["peak_score"]:.4f}',
                event["hit_count"],
                "; ".join(event["matched_labels"]),
            ]
        )


def load_daily_summary(path: Path):
    summary = defaultdict(
        lambda: defaultdict(lambda: {"event_count": 0, "total_duration_seconds": 0.0})
    )
    if not path.exists():
        return summary
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            day = row["date_utc"]
            event_type = row["event_type"]
            summary[day][event_type]["event_count"] = int(row["event_count"])
            summary[day][event_type]["total_duration_seconds"] = float(row["total_duration_seconds"])
    return summary


def utc_day(ts: dt.datetime) -> str:
    return ts.strftime("%Y-%m-%d")


def derive_source_id(cfg: dict[str, Any]) -> str:
    provider = str(nested_get(cfg, ("source", "provider"), "unknown"))
    if provider == "avfoundation":
        return f"avfoundation:{nested_get(cfg, ('source', 'device'), '')}"
    if provider == "rtsp":
        return f"rtsp:{nested_get(cfg, ('source', 'rtsp_url'), '')}"
    return provider


def normalize_taxon_key(value: str) -> str:
    lowered = value.strip().lower()
    return re.sub(r"\s+", " ", lowered)


def pick_first(row: dict[str, str], keys: tuple[str, ...]) -> str:
    for key in keys:
        if key in row and row[key] is not None:
            text = str(row[key]).strip()
            if text:
                return text
    return ""


def load_ebird_taxonomy_lookup(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    lookup: dict[str, str] = {}
    try:
        with path.open("r", newline="", encoding="utf-8", errors="ignore") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not isinstance(row, dict):
                    continue
                species_code = pick_first(row, ("species_code", "SPECIES_CODE"))
                common_name = pick_first(
                    row,
                    (
                        "primary_com_name",
                        "PRIMARY_COM_NAME",
                        "common_name",
                        "COMMON_NAME",
                    ),
                )
                scientific_name = pick_first(
                    row,
                    (
                        "sci_name",
                        "SCI_NAME",
                        "scientific_name",
                        "SCIENTIFIC_NAME",
                    ),
                )
                if not species_code:
                    continue
                species_code = species_code.lower()
                if common_name:
                    lookup[normalize_taxon_key(common_name)] = species_code
                if scientific_name:
                    lookup[normalize_taxon_key(scientific_name)] = species_code
    except Exception:
        return {}
    return lookup


def extract_ebird_species_code(det: dict[str, Any], lookup: dict[str, str]) -> str:
    # Prefer explicit provider fields when available; do not guess codes.
    for key in ("ebird_species_code", "species_code", "ebird_code"):
        value = det.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    # Fallback mapping by known label/common/scientific names.
    candidates = [
        det.get("label"),
        det.get("common_name"),
        det.get("scientific_name"),
    ]
    for candidate in candidates:
        if candidate is None:
            continue
        mapped = lookup.get(normalize_taxon_key(str(candidate)))
        if mapped:
            return mapped
    return ""


def write_daily_summary(path: Path, summary) -> None:
    rows = []
    for day in sorted(summary.keys()):
        for event_type in sorted(summary[day].keys()):
            stats = summary[day][event_type]
            count = stats["event_count"]
            total = stats["total_duration_seconds"]
            avg = total / count if count else 0.0
            rows.append(
                {
                    "date_utc": day,
                    "event_type": event_type,
                    "event_count": count,
                    "total_duration_seconds": round(total, 2),
                    "avg_duration_seconds": round(avg, 2),
                }
            )
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


def pcm16_to_float(raw: bytes) -> np.ndarray:
    arr = np.frombuffer(raw, dtype=np.int16).astype(np.float32)
    if arr.size == 0:
        return np.array([], dtype=np.float32)
    return arr / 32768.0


def resample_linear(samples: np.ndarray, src_hz: int, dst_hz: int) -> np.ndarray:
    if src_hz == dst_hz:
        return samples
    if samples.size == 0:
        return samples
    src_n = samples.size
    dst_n = max(1, int(round(src_n * (dst_hz / src_hz))))
    src_x = np.linspace(0.0, 1.0, src_n, endpoint=False)
    dst_x = np.linspace(0.0, 1.0, dst_n, endpoint=False)
    out = np.interp(dst_x, src_x, samples)
    return out.astype(np.float32, copy=False)


class RingBuffer:
    def __init__(self, sample_rate_hz: int, seconds: int):
        self.sample_rate_hz = sample_rate_hz
        self.capacity = sample_rate_hz * seconds
        self.data = np.zeros(self.capacity, dtype=np.float32)
        self.write_idx = 0
        self.size = 0

    def append(self, samples: np.ndarray) -> int:
        if samples.size == 0:
            return 0
        if samples.size >= self.capacity:
            samples = samples[-self.capacity :]
        overwritten = max(0, self.size + samples.size - self.capacity)

        first = min(samples.size, self.capacity - self.write_idx)
        self.data[self.write_idx : self.write_idx + first] = samples[:first]
        remain = samples.size - first
        if remain > 0:
            self.data[0:remain] = samples[first:]

        self.write_idx = (self.write_idx + samples.size) % self.capacity
        self.size = min(self.capacity, self.size + samples.size)
        return overwritten

    def latest(self, sample_count: int) -> np.ndarray | None:
        if sample_count <= 0:
            return np.array([], dtype=np.float32)
        if self.size < sample_count:
            return None

        start = (self.write_idx - sample_count) % self.capacity
        end = self.write_idx
        if start < end:
            return self.data[start:end].copy()
        return np.concatenate((self.data[start:], self.data[:end])).copy()


def start_ffmpeg(cfg: dict[str, Any]) -> subprocess.Popen:
    ffmpeg_bin = str(nested_get(cfg, ("source", "ffmpeg_bin"), "ffmpeg"))
    provider = str(nested_get(cfg, ("source", "provider")))
    sample_rate_hz = int(nested_get(cfg, ("source", "sample_rate_hz")))

    if provider == "avfoundation":
        input_args = ["-f", "avfoundation", "-i", str(nested_get(cfg, ("source", "device"), ""))]
    else:
        input_args = [
            "-rtsp_transport",
            "tcp",
            "-i",
            str(nested_get(cfg, ("source", "rtsp_url"), "")),
        ]

    cmd = [
        ffmpeg_bin,
        "-hide_banner",
        "-loglevel",
        "error",
        *input_args,
        "-vn",
        "-ac",
        "1",
        "-ar",
        str(sample_rate_hz),
        "-f",
        "s16le",
        "-acodec",
        "pcm_s16le",
        "-",
    ]
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)


def stop_ffmpeg(proc: subprocess.Popen | None) -> None:
    if proc is None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=2.0)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


def read_ffmpeg_stderr(proc: subprocess.Popen | None) -> str:
    if proc is None or proc.stderr is None:
        return ""
    try:
        return proc.stderr.read().decode("utf-8", errors="ignore")
    except Exception:
        return ""


@dataclass
class IngestMetrics:
    report_interval_seconds: float
    start_monotonic: float
    next_report_monotonic: float
    chunks: int = 0
    samples_ingested: int = 0
    samples_overwritten: int = 0
    reconnects: int = 0

    def record_chunk(self, sample_count: int, overwritten: int) -> None:
        self.chunks += 1
        self.samples_ingested += sample_count
        self.samples_overwritten += overwritten

    def should_report(self, now_monotonic: float) -> bool:
        return now_monotonic >= self.next_report_monotonic

    def advance_report_window(self, now_monotonic: float) -> None:
        self.next_report_monotonic = now_monotonic + self.report_interval_seconds

    def render(self, now_monotonic: float) -> str:
        elapsed = max(0.001, now_monotonic - self.start_monotonic)
        ingest_hz = self.samples_ingested / elapsed
        return (
            f"ingest chunks={self.chunks} samples={self.samples_ingested} "
            f"sample_rate_observed={ingest_hz:.1f}Hz "
            f"reconnects={self.reconnects} overwritten_samples={self.samples_overwritten}"
        )


@dataclass
class BranchRuntime:
    name: str
    sample_rate_hz: int
    window_seconds: float
    hop_seconds: float
    next_due: float
    pending_due: float | None = None
    enqueued_jobs: int = 0
    run_jobs: int = 0
    dropped_jobs: int = 0
    lag_seconds_total: float = 0.0
    lag_seconds_max: float = 0.0

    def enqueue_due(self, *, now_monotonic: float, drop_policy: str) -> None:
        scheduled = 0
        while now_monotonic >= self.next_due and scheduled < 1000:
            if self.pending_due is None:
                self.pending_due = self.next_due
                self.enqueued_jobs += 1
            else:
                if drop_policy == "drop_oldest_branch_job":
                    self.pending_due = self.next_due
                self.dropped_jobs += 1
            self.next_due += self.hop_seconds
            scheduled += 1

    def run_one(
        self,
        *,
        ring: RingBuffer,
        canonical_sample_rate_hz: int,
        now_monotonic: float,
        now_utc: dt.datetime,
        dump_top: bool,
    ) -> None:
        if self.pending_due is None:
            return

        wanted = int(round(self.window_seconds * canonical_sample_rate_hz))
        canonical_window = ring.latest(wanted)
        if canonical_window is None:
            return

        lag_seconds = max(0.0, now_monotonic - self.pending_due)
        self.lag_seconds_total += lag_seconds
        self.lag_seconds_max = max(self.lag_seconds_max, lag_seconds)

        window = resample_linear(canonical_window, canonical_sample_rate_hz, self.sample_rate_hz)
        rms = float(np.sqrt(np.mean(np.square(window)))) if window.size else 0.0
        peak = float(np.max(np.abs(window))) if window.size else 0.0

        if dump_top:
            print(
                f"[{iso(now_utc)}] branch={self.name} "
                f"window={self.window_seconds:.1f}s hop={self.hop_seconds:.1f}s "
                f"rms={rms:.4f} peak={peak:.4f} n={window.size} "
                f"lag={lag_seconds:.3f}s"
            )
        self.pending_due = None
        self.run_jobs += 1

    def health_line(self) -> str:
        avg_lag = (self.lag_seconds_total / self.run_jobs) if self.run_jobs else 0.0
        return (
            f"branch={self.name} enqueued={self.enqueued_jobs} run={self.run_jobs} "
            f"dropped={self.dropped_jobs} pending={'yes' if self.pending_due is not None else 'no'} "
            f"avg_lag_s={avg_lag:.3f} max_lag_s={self.lag_seconds_max:.3f}"
        )

    def finalize(self) -> None:
        return


class YAMNetLocalWriter:
    def __init__(self, events_csv: Path, daily_csv: Path):
        self.events_csv = events_csv
        self.daily_csv = daily_csv
        ensure_events_csv(self.events_csv)
        self.daily_summary = load_daily_summary(self.daily_csv)
        write_daily_summary(self.daily_csv, self.daily_summary)

    def write_event(self, event: dict) -> None:
        append_event(self.events_csv, event)
        day = utc_day(event["start"])
        self.daily_summary[day][event["event_type"]]["event_count"] += 1
        self.daily_summary[day][event["event_type"]]["total_duration_seconds"] += event["duration_seconds"]
        write_daily_summary(self.daily_csv, self.daily_summary)


def firebase_url(base_url: str, path: str, auth_token: str) -> str:
    clean_base = base_url.rstrip("/")
    parts = [urllib.parse.quote(part, safe="") for part in path.strip("/").split("/") if part]
    full = f"{clean_base}/{'/'.join(parts)}.json"
    if auth_token:
        full = f"{full}?auth={urllib.parse.quote(auth_token, safe='')}"
    return full


class FirebaseIdTokenManager:
    def __init__(self, *, api_key: str, refresh_token: str, id_token: str = ""):
        self.api_key = api_key.strip()
        self.refresh_token = refresh_token.strip()
        self.id_token = id_token.strip()
        self.expires_at_monotonic = 0.0

    def refresh(self) -> str:
        if not self.api_key or not self.refresh_token:
            raise RuntimeError("firebase token refresh requires api_key and refresh_token")
        token_url = f"https://securetoken.googleapis.com/v1/token?key={urllib.parse.quote(self.api_key, safe='')}"
        body = urllib.parse.urlencode(
            {
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
            }
        ).encode("utf-8")
        req = urllib.request.Request(
            token_url,
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            raw = json.loads(resp.read().decode("utf-8", errors="ignore"))
        self.id_token = str(raw.get("id_token", "")).strip()
        self.refresh_token = str(raw.get("refresh_token", self.refresh_token)).strip()
        expires_in = int(raw.get("expires_in", 3600))
        self.expires_at_monotonic = time.monotonic() + max(30, expires_in - 120)
        if not self.id_token:
            raise RuntimeError("firebase refresh did not return id_token")
        return self.id_token

    def get_token(self) -> str:
        if not self.id_token or time.monotonic() >= self.expires_at_monotonic:
            return self.refresh()
        return self.id_token


def firebase_sign_in_with_password(*, api_key: str, email: str, password: str) -> dict[str, Any]:
    url = (
        "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword"
        f"?key={urllib.parse.quote(api_key, safe='')}"
    )
    body = {
        "email": email,
        "password": password,
        "returnSecureToken": True,
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=8) as resp:
        return json.loads(resp.read().decode("utf-8", errors="ignore"))


def firebase_request(
    base_url: str,
    path: str,
    payload: dict,
    method: str,
    auth_token: str = "",
    token_manager: FirebaseIdTokenManager | None = None,
) -> None:
    token = auth_token
    if token_manager is not None:
        token = token_manager.get_token()
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        firebase_url(base_url, path, token),
        data=data,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            resp.read()
        return
    except urllib.error.HTTPError as exc:
        if exc.code in {401, 403} and token_manager is not None:
            refreshed = token_manager.refresh()
            retry_req = urllib.request.Request(
                firebase_url(base_url, path, refreshed),
                data=data,
                headers={"Content-Type": "application/json"},
                method=method,
            )
            with urllib.request.urlopen(retry_req, timeout=5) as resp:
                resp.read()
            return
        raise


class FirebaseRestWriter:
    def __init__(
        self,
        *,
        base_url: str,
        auth_token: str,
        site_id: str,
        base_path: str,
        token_manager: FirebaseIdTokenManager | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.auth_token = auth_token
        self.site_id = site_id
        self.base_path = base_path.strip("/").strip()
        self.token_manager = token_manager

    def _site_root(self) -> str:
        return f"{self.base_path}/{self.site_id}" if self.base_path else self.site_id

    def write_event(self, event: dict, day_count: int, day_total_duration: float) -> None:
        event_payload = {
            "site_id": self.site_id,
            "source_id": str(event.get("source_id", "")),
            "model": str(event.get("model", "yamnet")),
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
        firebase_request(
            self.base_url,
            f"{self._site_root()}/events",
            event_payload,
            method="POST",
            auth_token=self.auth_token,
            token_manager=self.token_manager,
        )

        daily_payload = {
            "site_id": self.site_id,
            "source_id": str(event.get("source_id", "")),
            "model": str(event.get("model", "yamnet")),
            "date_utc": utc_day(event["start"]),
            "event_type": event["event_type"],
            "event_count": int(day_count),
            "total_duration_seconds": round(float(day_total_duration), 2),
            "avg_duration_seconds": round(float(day_total_duration) / day_count, 2) if day_count else 0.0,
            "updated_at_utc": iso(utc_now()),
        }
        firebase_request(
            self.base_url,
            f"{self._site_root()}/daily_summary/{utc_day(event['start'])}/{event['event_type']}",
            daily_payload,
            method="PUT",
            auth_token=self.auth_token,
            token_manager=self.token_manager,
        )

    def write_bird_detection(self, detection: dict) -> None:
        payload = dict(detection)
        payload["site_id"] = self.site_id
        payload["model"] = "birdnet"
        payload["ingested_at_utc"] = iso(utc_now())
        firebase_request(
            self.base_url,
            f"{self._site_root()}/birds",
            payload,
            method="POST",
            auth_token=self.auth_token,
            token_manager=self.token_manager,
        )


class FirebaseAdminWriter:
    def __init__(self, *, service_account_path: Path, db_url: str, site_id: str, base_path: str):
        if firebase_admin is None or firebase_credentials is None or firebase_db is None:
            raise RuntimeError("firebase-admin is not installed. Run: pip install firebase-admin")
        if not service_account_path.exists():
            raise RuntimeError(f"Firebase service account file not found: {service_account_path}")
        if not db_url:
            raise RuntimeError("Firebase Realtime DB URL required for Admin SDK writes")
        self.site_id = site_id
        self.base_path = base_path.strip("/").strip()
        app_name = f"orecchio-{service_account_path.resolve()}-{site_id}"
        app = None
        for existing in firebase_admin._apps.values():
            if existing.name == app_name:
                app = existing
                break
        if app is None:
            cred = firebase_credentials.Certificate(str(service_account_path))
            app = firebase_admin.initialize_app(cred, {"databaseURL": db_url}, name=app_name)
        self.app = app

    def _site_root(self) -> str:
        return f"{self.base_path}/{self.site_id}" if self.base_path else self.site_id

    def write_event(self, event: dict, day_count: int, day_total_duration: float) -> None:
        event_payload = {
            "site_id": self.site_id,
            "source_id": str(event.get("source_id", "")),
            "model": str(event.get("model", "yamnet")),
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
        firebase_db.reference(f"{self._site_root()}/events", app=self.app).push(event_payload)

        daily_payload = {
            "site_id": self.site_id,
            "source_id": str(event.get("source_id", "")),
            "model": str(event.get("model", "yamnet")),
            "date_utc": utc_day(event["start"]),
            "event_type": event["event_type"],
            "event_count": int(day_count),
            "total_duration_seconds": round(float(day_total_duration), 2),
            "avg_duration_seconds": round(float(day_total_duration) / day_count, 2) if day_count else 0.0,
            "updated_at_utc": iso(utc_now()),
        }
        firebase_db.reference(
            f"{self._site_root()}/daily_summary/{utc_day(event['start'])}/{event['event_type']}",
            app=self.app,
        ).set(daily_payload)

    def write_bird_detection(self, detection: dict) -> None:
        payload = dict(detection)
        payload["site_id"] = self.site_id
        payload["model"] = "birdnet"
        payload["ingested_at_utc"] = iso(utc_now())
        firebase_db.reference(f"{self._site_root()}/birds", app=self.app).push(payload)


class YAMNetWriterStack:
    def __init__(
        self,
        *,
        local: YAMNetLocalWriter,
        firebase_writer: FirebaseRestWriter | FirebaseAdminWriter | None,
        site_id: str,
        source_id: str,
    ):
        self.local = local
        self.firebase_writer = firebase_writer
        self.site_id = site_id
        self.source_id = source_id

    def write_event(self, event: dict) -> None:
        enriched = dict(event)
        enriched["site_id"] = self.site_id
        enriched["source_id"] = self.source_id
        enriched["model"] = "yamnet"
        self.local.write_event(enriched)
        if self.firebase_writer is None:
            return
        day = utc_day(enriched["start"])
        stats = self.local.daily_summary[day][enriched["event_type"]]
        try:
            self.firebase_writer.write_event(
                enriched,
                day_count=stats["event_count"],
                day_total_duration=stats["total_duration_seconds"],
            )
        except Exception as exc:
            print(f"[{iso(utc_now())}] firebase write failed: {exc}", file=sys.stderr)


class BirdNETLocalWriter:
    def __init__(self, detections_jsonl: Path):
        self.detections_jsonl = detections_jsonl
        self.detections_jsonl.parent.mkdir(parents=True, exist_ok=True)
        if not self.detections_jsonl.exists():
            self.detections_jsonl.touch()

    def write_detection(self, detection: dict) -> None:
        with self.detections_jsonl.open("a", encoding="utf-8") as f:
            f.write(json.dumps(detection, separators=(",", ":")))
            f.write("\n")


class BirdWeatherQueueExporter:
    def __init__(
        self,
        *,
        enabled: bool,
        station_id: str,
        api_token: str,
        require_manual_review: bool,
        queue_jsonl: Path,
        sent_jsonl: Path,
    ):
        self.enabled = enabled
        self.station_id = station_id
        self.api_token = api_token
        self.require_manual_review = require_manual_review
        self.queue_jsonl = queue_jsonl
        self.sent_jsonl = sent_jsonl
        self.queue_jsonl.parent.mkdir(parents=True, exist_ok=True)
        self.sent_jsonl.parent.mkdir(parents=True, exist_ok=True)
        if not self.queue_jsonl.exists():
            self.queue_jsonl.touch()
        if not self.sent_jsonl.exists():
            self.sent_jsonl.touch()
        self.queued = 0
        self.sent = 0

    def enqueue(self, detection: dict) -> None:
        if not self.enabled:
            return
        record = {
            "status": "pending_review" if self.require_manual_review else "ready_auto",
            "queued_at_utc": iso(utc_now()),
            "target": {
                "provider": "birdweather",
                "station_id": self.station_id,
            },
            "detection": detection,
            "clip": {
                "format": "flac",
                "start_utc": detection.get("start_utc"),
                "end_utc": detection.get("end_utc"),
                "path": None,
            },
        }
        with self.queue_jsonl.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, separators=(",", ":")))
            f.write("\n")
        self.queued += 1

    def flush_auto_ready(self) -> int:
        if not self.enabled or self.require_manual_review:
            return 0
        moved = 0
        remaining: list[str] = []
        with self.queue_jsonl.open("r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
        for line in lines:
            raw = line.strip()
            if not raw:
                continue
            try:
                rec = json.loads(raw)
            except Exception:
                remaining.append(raw)
                continue
            if rec.get("status") != "ready_auto":
                remaining.append(raw)
                continue
            rec["status"] = "sent_local_placeholder"
            rec["sent_at_utc"] = iso(utc_now())
            with self.sent_jsonl.open("a", encoding="utf-8") as sf:
                sf.write(json.dumps(rec, separators=(",", ":")))
                sf.write("\n")
            moved += 1
        with self.queue_jsonl.open("w", encoding="utf-8") as f:
            for raw in remaining:
                f.write(raw)
                f.write("\n")
        self.sent += moved
        return moved

    def health_line(self) -> str:
        mode = "manual_review" if self.require_manual_review else "auto_queue"
        return f"birdweather exporter mode={mode} queued={self.queued} moved_to_sent={self.sent}"


class BirdNETWriterStack:
    def __init__(
        self,
        *,
        local: BirdNETLocalWriter,
        exporter: BirdWeatherQueueExporter | None,
        firebase_writer: FirebaseRestWriter | FirebaseAdminWriter | None,
    ):
        self.local = local
        self.exporter = exporter
        self.firebase_writer = firebase_writer

    def write_detection(self, detection: dict) -> None:
        self.local.write_detection(detection)
        if self.firebase_writer is not None:
            try:
                self.firebase_writer.write_bird_detection(detection)
            except Exception as exc:
                print(f"[{iso(utc_now())}] firebase bird write failed: {exc}", file=sys.stderr)
        if self.exporter is not None:
            self.exporter.enqueue(detection)

    def flush(self) -> None:
        if self.exporter is not None:
            self.exporter.flush_auto_ready()

    def health_line(self) -> str | None:
        if self.exporter is None:
            return None
        return self.exporter.health_line()


@dataclass
class YAMNetBranch:
    name: str
    sample_rate_hz: int
    window_seconds: float
    hop_seconds: float
    top_k: int
    tracker: EventTracker
    rules: dict[str, EventRule]
    regimes: dict[str, RollingRatioDetector]
    writer: YAMNetWriterStack
    next_due: float
    pending_due: float | None = None
    enqueued_jobs: int = 0
    run_jobs: int = 0
    dropped_jobs: int = 0
    lag_seconds_total: float = 0.0
    lag_seconds_max: float = 0.0
    model: Any = None
    class_names: list[str] | None = None
    tf: Any = None

    def _ensure_model(self) -> None:
        if self.model is not None:
            return
        self.model, self.class_names, self.tf = load_yamnet_model()

    def enqueue_due(self, *, now_monotonic: float, drop_policy: str) -> None:
        scheduled = 0
        while now_monotonic >= self.next_due and scheduled < 1000:
            if self.pending_due is None:
                self.pending_due = self.next_due
                self.enqueued_jobs += 1
            else:
                if drop_policy == "drop_oldest_branch_job":
                    self.pending_due = self.next_due
                self.dropped_jobs += 1
            self.next_due += self.hop_seconds
            scheduled += 1

    def run_one(
        self,
        *,
        ring: RingBuffer,
        canonical_sample_rate_hz: int,
        now_monotonic: float,
        now_utc: dt.datetime,
        dump_top: bool,
    ) -> None:
        if self.pending_due is None:
            return
        wanted = int(round(self.window_seconds * canonical_sample_rate_hz))
        canonical_window = ring.latest(wanted)
        if canonical_window is None:
            return
        lag_seconds = max(0.0, now_monotonic - self.pending_due)
        self.lag_seconds_total += lag_seconds
        self.lag_seconds_max = max(self.lag_seconds_max, lag_seconds)
        waveform = resample_linear(canonical_window, canonical_sample_rate_hz, self.sample_rate_hz)
        self._ensure_model()
        top = score_yamnet_clip(self.model, self.class_names, self.tf, waveform, top_k=self.top_k)

        if dump_top:
            pretty = ", ".join(f"{label}={score:.3f}" for label, score in top)
            print(f"[{iso(now_utc)}] yamnet top: {pretty}")

        regime_states: dict[str, tuple[bool, float, str]] = {}
        for event_type, detector in self.regimes.items():
            active, score = detector.observe(now_utc, top)
            label = (
                f"{detector.label_name}>={detector.hit_threshold:.2f} "
                f"(rolling {int(round(detector.window_seconds / 60.0))}m ratio)"
            )
            regime_states[event_type] = (active, score, label)

        matches = classify_yamnet(top, self.rules)
        mower_active = regime_states.get("mower", (False, 0.0, ""))[0]
        for event_type, matched_label, score in matches:
            if event_type == "revving" and mower_active:
                continue
            self.tracker.update(event_type, matched_label, score, now_utc)

        for event_type, (active, score, label) in regime_states.items():
            if active:
                self.tracker.update(event_type, label, score, now_utc)

        for event in self.tracker.flush_finished(now_utc):
            self.writer.write_event(event)
            print(
                f"[{iso(utc_now())}] closed "
                f"{event['event_type']} duration={event['duration_seconds']:.1f}s "
                f"peak={event['peak_score']:.3f} hits={event['hit_count']} "
                f"labels={event['matched_labels']}"
            )

        self.pending_due = None
        self.run_jobs += 1

    def health_line(self) -> str:
        avg_lag = (self.lag_seconds_total / self.run_jobs) if self.run_jobs else 0.0
        return (
            f"branch={self.name} enqueued={self.enqueued_jobs} run={self.run_jobs} "
            f"dropped={self.dropped_jobs} pending={'yes' if self.pending_due is not None else 'no'} "
            f"avg_lag_s={avg_lag:.3f} max_lag_s={self.lag_seconds_max:.3f}"
        )

    def finalize(self) -> None:
        for event in self.tracker.flush_all():
            self.writer.write_event(event)
            print(
                f"[{iso(utc_now())}] closed "
                f"{event['event_type']} duration={event['duration_seconds']:.1f}s "
                f"peak={event['peak_score']:.3f} hits={event['hit_count']} "
                f"labels={event['matched_labels']}"
            )


@dataclass
class BirdNETBranch:
    name: str
    site_id: str
    source_id: str
    sample_rate_hz: int
    window_seconds: float
    hop_seconds: float
    min_confidence: float
    log_zero_detections: bool
    lat: float | None
    lon: float | None
    date_mode: str
    ebird_lookup: dict[str, str]
    writer: BirdNETWriterStack
    next_due: float
    pending_due: float | None = None
    enqueued_jobs: int = 0
    run_jobs: int = 0
    dropped_jobs: int = 0
    lag_seconds_total: float = 0.0
    lag_seconds_max: float = 0.0
    detection_count: int = 0
    analyzer: Any = None
    recording_buffer_class: Any = None
    disabled_reason: str | None = None

    def _ensure_model(self) -> None:
        if self.analyzer is not None and self.recording_buffer_class is not None:
            return
        try:
            from birdnetlib import RecordingBuffer
            from birdnetlib.analyzer import Analyzer
        except Exception as exc:
            self.disabled_reason = (
                "birdnet unavailable: missing dependencies. "
                "Install with: pip install birdnetlib librosa"
            )
            raise RuntimeError(
                self.disabled_reason
            ) from exc
        self.recording_buffer_class = RecordingBuffer
        # birdnetlib can be noisy on stdout/stderr; keep logs focused.
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            self.analyzer = Analyzer()  # Auto-downloads/checks model assets on first use.

    def enqueue_due(self, *, now_monotonic: float, drop_policy: str) -> None:
        scheduled = 0
        while now_monotonic >= self.next_due and scheduled < 1000:
            if self.pending_due is None:
                self.pending_due = self.next_due
                self.enqueued_jobs += 1
            else:
                if drop_policy == "drop_oldest_branch_job":
                    self.pending_due = self.next_due
                self.dropped_jobs += 1
            self.next_due += self.hop_seconds
            scheduled += 1

    def run_one(
        self,
        *,
        ring: RingBuffer,
        canonical_sample_rate_hz: int,
        now_monotonic: float,
        now_utc: dt.datetime,
        dump_top: bool,
    ) -> None:
        if self.disabled_reason is not None:
            self.pending_due = None
            return
        if self.pending_due is None:
            return
        wanted = int(round(self.window_seconds * canonical_sample_rate_hz))
        canonical_window = ring.latest(wanted)
        if canonical_window is None:
            return

        lag_seconds = max(0.0, now_monotonic - self.pending_due)
        self.lag_seconds_total += lag_seconds
        self.lag_seconds_max = max(self.lag_seconds_max, lag_seconds)
        waveform = resample_linear(canonical_window, canonical_sample_rate_hz, self.sample_rate_hz)
        window_start = now_utc - dt.timedelta(seconds=self.window_seconds)

        try:
            self._ensure_model()
        except Exception as exc:
            print(f"[{iso(utc_now())}] {exc}", file=sys.stderr)
            self.pending_due = None
            return
        if self.date_mode == "window_start_utc":
            birdnet_date = window_start.replace(tzinfo=None)
        elif self.date_mode == "today_utc":
            now = utc_now()
            birdnet_date = dt.datetime(year=now.year, month=now.month, day=now.day)
        else:
            birdnet_date = None

        recording_kwargs: dict[str, Any] = {
            "min_conf": self.min_confidence,
        }
        if self.lat is not None:
            recording_kwargs["lat"] = self.lat
        if self.lon is not None:
            recording_kwargs["lon"] = self.lon
        if birdnet_date is not None:
            recording_kwargs["date"] = birdnet_date

        recording = self.recording_buffer_class(
            self.analyzer,
            waveform,
            self.sample_rate_hz,
            **recording_kwargs,
        )
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            recording.analyze()
        detections = getattr(recording, "detections", []) or []

        for det in detections:
            start_offset = float(det.get("start_time", 0.0))
            end_offset = float(det.get("end_time", self.window_seconds))
            confidence = float(det.get("confidence", 0.0))
            label = str(det.get("label") or det.get("common_name") or det.get("scientific_name") or "unknown")
            ebird_species_code = extract_ebird_species_code(det, self.ebird_lookup)
            payload = {
                "site_id": self.site_id,
                "source_id": self.source_id,
                "model": "birdnet",
                "label": label,
                "ebird_species_code": ebird_species_code,
                "score": round(confidence, 6),
                "start_utc": iso(window_start + dt.timedelta(seconds=start_offset)),
                "end_utc": iso(window_start + dt.timedelta(seconds=end_offset)),
                "metadata": {
                    "common_name": det.get("common_name"),
                    "scientific_name": det.get("scientific_name"),
                    "window_seconds": self.window_seconds,
                    "hop_seconds": self.hop_seconds,
                    "min_confidence": self.min_confidence,
                },
            }
            self.writer.write_detection(payload)
            self.detection_count += 1

        self.writer.flush()

        if len(detections) > 0 or (self.log_zero_detections and len(detections) == 0):
            details = ""
            if len(detections) > 0:
                ranked = sorted(detections, key=lambda d: float(d.get("confidence", 0.0)), reverse=True)
                preview = []
                for det in ranked[:3]:
                    label = str(
                        det.get("label")
                        or det.get("common_name")
                        or det.get("scientific_name")
                        or "unknown"
                    )
                    conf = float(det.get("confidence", 0.0))
                    preview.append(f"{label}={conf:.3f}")
                details = f" top={', '.join(preview)}"
            print(
                f"[{iso(now_utc)}] birdnet detections={len(detections)} "
                f"window={self.window_seconds:.1f}s hop={self.hop_seconds:.1f}s lag={lag_seconds:.3f}s"
                f"{details}"
            )

        self.pending_due = None
        self.run_jobs += 1

    def health_line(self) -> str:
        if self.disabled_reason is not None:
            return f"branch={self.name} disabled reason={self.disabled_reason}"
        avg_lag = (self.lag_seconds_total / self.run_jobs) if self.run_jobs else 0.0
        line = (
            f"branch={self.name} enqueued={self.enqueued_jobs} run={self.run_jobs} "
            f"dropped={self.dropped_jobs} pending={'yes' if self.pending_due is not None else 'no'} "
            f"avg_lag_s={avg_lag:.3f} max_lag_s={self.lag_seconds_max:.3f} "
            f"detections={self.detection_count}"
        )
        exporter_line = self.writer.health_line()
        if exporter_line:
            return f"{line} | {exporter_line}"
        return line

    def finalize(self) -> None:
        self.writer.flush()


def build_firebase_writer(cfg: dict[str, Any]) -> FirebaseRestWriter | FirebaseAdminWriter | None:
    firebase_enabled = bool(nested_get(cfg, ("outputs", "firebase", "enabled"), False))
    if not firebase_enabled:
        return None
    site_id = str(nested_get(cfg, ("site", "id"), ""))
    base_path = str(nested_get(cfg, ("outputs", "firebase", "base_path"), "orecchio_sites"))
    db_url = str(nested_get(cfg, ("outputs", "firebase", "db_url"), "")).strip().rstrip("/")
    auth_token = str(nested_get(cfg, ("outputs", "firebase", "auth_token"), "")).strip()
    api_key = str(nested_get(cfg, ("outputs", "firebase", "api_key"), "")).strip()
    refresh_token = str(nested_get(cfg, ("outputs", "firebase", "refresh_token"), "")).strip()
    id_token = str(nested_get(cfg, ("outputs", "firebase", "id_token"), "")).strip()
    service_account = str(nested_get(cfg, ("outputs", "firebase", "service_account"), "")).strip()
    if service_account:
        try:
            writer = FirebaseAdminWriter(
                service_account_path=Path(service_account),
                db_url=db_url,
                site_id=site_id,
                base_path=base_path,
            )
            print(f"[{iso(utc_now())}] firebase enabled (admin-sdk): {base_path}/{site_id}/...")
            return writer
        except Exception as exc:
            print(f"[{iso(utc_now())}] firebase admin init failed: {exc}", file=sys.stderr)
            return None
    if db_url:
        token_manager = None
        auth_mode = "static-token"
        if api_key and refresh_token:
            token_manager = FirebaseIdTokenManager(
                api_key=api_key,
                refresh_token=refresh_token,
                id_token=id_token,
            )
            auth_mode = "refresh-token"
        writer = FirebaseRestWriter(
            base_url=db_url,
            auth_token=auth_token,
            site_id=site_id,
            base_path=base_path,
            token_manager=token_manager,
        )
        print(f"[{iso(utc_now())}] firebase enabled (rest/{auth_mode}): {base_path}/{site_id}/...")
        return writer
    print(
        f"[{iso(utc_now())}] firebase enabled=true but outputs.firebase.db_url is empty; "
        "skipping firebase writes",
        file=sys.stderr,
    )
    return None


def build_branches(cfg: dict[str, Any]) -> list[Any]:
    out: list[Any] = []
    enabled = nested_get(cfg, ("branches", "enabled"), [])
    if isinstance(enabled, str):
        enabled = parse_csv_list(enabled)
    if not isinstance(enabled, list):
        enabled = []
    now_mono = time.monotonic()
    firebase_writer = build_firebase_writer(cfg)
    site_id = str(nested_get(cfg, ("site", "id"), ""))
    source_id = derive_source_id(cfg)

    for name in enabled:
        section = nested_get(cfg, ("branches", name), {})
        if not isinstance(section, dict):
            continue
        if not bool(section.get("enabled", False)):
            continue
        sample_rate_hz = int(section["sample_rate_hz"])
        window_seconds = float(section["window_seconds"])
        hop_seconds = float(section["hop_seconds"])
        if name == "yamnet":
            top_k = int(section.get("top_k", 20))
            yamnet_rules = build_yamnet_rules()
            local_writer = YAMNetLocalWriter(
                events_csv=Path(str(nested_get(cfg, ("outputs", "local", "events_csv"), "orecchio_events.csv"))),
                daily_csv=Path(str(nested_get(cfg, ("outputs", "local", "daily_csv"), "orecchio_daily_summary.csv"))),
            )

            writer = YAMNetWriterStack(
                local=local_writer,
                firebase_writer=firebase_writer,
                site_id=site_id,
                source_id=source_id,
            )
            out.append(
                YAMNetBranch(
                    name=name,
                    sample_rate_hz=sample_rate_hz,
                    window_seconds=window_seconds,
                    hop_seconds=hop_seconds,
                    top_k=top_k,
                    tracker=EventTracker(yamnet_rules, chunk_seconds=window_seconds),
                    rules=yamnet_rules,
                    regimes=build_yamnet_regimes(window_seconds),
                    writer=writer,
                    next_due=now_mono + window_seconds,
                )
            )
            continue
        if name == "birdnet":
            min_confidence = float(section.get("min_confidence", 0.25))
            log_zero_detections = bool(section.get("log_zero_detections", False))
            lat = section.get("lat")
            lon = section.get("lon")
            date_mode = str(section.get("date_mode", "window_start_utc"))
            ebird_taxonomy_csv = str(section.get("ebird_taxonomy_csv", "ebird_taxonomy.csv")).strip()
            ebird_lookup = {}
            if ebird_taxonomy_csv:
                csv_path = Path(ebird_taxonomy_csv)
                ebird_lookup = load_ebird_taxonomy_lookup(csv_path)
                if ebird_lookup:
                    print(
                        f"[{iso(utc_now())}] birdnet taxonomy loaded: "
                        f"{csv_path} ({len(ebird_lookup)} name->code entries)"
                    )
                else:
                    print(
                        f"[{iso(utc_now())}] birdnet taxonomy not loaded from {csv_path}; "
                        "ebird_species_code may be blank",
                        file=sys.stderr,
                    )
            local_writer = BirdNETLocalWriter(
                detections_jsonl=Path(
                    str(nested_get(cfg, ("outputs", "local", "detections_jsonl"), "orecchio_detections.jsonl"))
                )
            )
            birdweather_exporter: BirdWeatherQueueExporter | None = None
            birdweather_enabled = bool(nested_get(cfg, ("outputs", "birdweather", "enabled"), False))
            if birdweather_enabled:
                birdweather_exporter = BirdWeatherQueueExporter(
                    enabled=True,
                    station_id=str(nested_get(cfg, ("outputs", "birdweather", "station_id"), "")).strip(),
                    api_token=str(nested_get(cfg, ("outputs", "birdweather", "api_token"), "")).strip(),
                    require_manual_review=bool(
                        nested_get(cfg, ("outputs", "birdweather", "require_manual_review"), True)
                    ),
                    queue_jsonl=Path(
                        str(
                            nested_get(
                                cfg,
                                ("outputs", "birdweather", "queue_jsonl"),
                                "orecchio_birdweather_queue.jsonl",
                            )
                        )
                    ),
                    sent_jsonl=Path(
                        str(
                            nested_get(
                                cfg,
                                ("outputs", "birdweather", "sent_jsonl"),
                                "orecchio_birdweather_sent.jsonl",
                            )
                        )
                    ),
                )
                mode = "manual-review" if birdweather_exporter.require_manual_review else "auto-queue"
                print(f"[{iso(utc_now())}] birdweather exporter enabled ({mode})")
            out.append(
                BirdNETBranch(
                    name=name,
                    site_id=site_id,
                    source_id=source_id,
                    sample_rate_hz=sample_rate_hz,
                    window_seconds=window_seconds,
                    hop_seconds=hop_seconds,
                    min_confidence=min_confidence,
                    log_zero_detections=log_zero_detections,
                    lat=float(lat) if lat is not None else None,
                    lon=float(lon) if lon is not None else None,
                    date_mode=date_mode,
                    ebird_lookup=ebird_lookup,
                    writer=BirdNETWriterStack(
                        local=local_writer,
                        exporter=birdweather_exporter,
                        firebase_writer=firebase_writer,
                    ),
                    next_due=now_mono + window_seconds,
                )
            )
            continue
        out.append(
            BranchRuntime(
                name=name,
                sample_rate_hz=sample_rate_hz,
                window_seconds=window_seconds,
                hop_seconds=hop_seconds,
                next_due=now_mono + window_seconds,
            )
        )
    return out


def load_config(args: argparse.Namespace) -> dict[str, Any]:
    dotenv = load_dotenv(Path(".env"))
    cfg = default_config()
    cfg = deep_merge(cfg, load_toml_config(Path(args.config)))
    cfg = apply_env_overrides(cfg, dotenv)
    cfg = apply_env_overrides(cfg, dict(os.environ))
    cfg = apply_cli_overrides(cfg, args)
    return cfg


def run_login_command(args: argparse.Namespace, cfg: dict[str, Any]) -> int:
    api_key = (
        args.firebase_api_key
        or str(nested_get(cfg, ("outputs", "firebase", "api_key"), "")).strip()
    )
    email = args.firebase_email or ""
    password = args.firebase_password or ""
    if not api_key:
        print("firebase login requires --firebase-api-key (or outputs.firebase.api_key in config)", file=sys.stderr)
        return 2
    if not email:
        print("firebase login requires --firebase-email", file=sys.stderr)
        return 2
    if not password:
        print("firebase login requires --firebase-password", file=sys.stderr)
        return 2
    try:
        auth = firebase_sign_in_with_password(api_key=api_key, email=email, password=password)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        print(f"firebase login failed: HTTP {exc.code} {detail}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"firebase login failed: {exc}", file=sys.stderr)
        return 1

    refresh_token = str(auth.get("refreshToken", "")).strip()
    id_token = str(auth.get("idToken", "")).strip()
    local_id = str(auth.get("localId", "")).strip()
    if not refresh_token:
        print("firebase login did not return refreshToken", file=sys.stderr)
        return 1

    print("firebase login succeeded")
    print(f"email={email}")
    if local_id:
        print(f"local_id={local_id}")
    print("\nUse this in orecchio.toml:")
    print('[outputs.firebase]')
    print('auth_token = ""')
    print(f'api_key = "{api_key}"')
    print(f'refresh_token = "{refresh_token}"')
    if id_token:
        print(f'id_token = "{id_token}"')
    print('service_account = ""')
    return 0


def upsert_toml_key_in_section(path: Path, section: str, key: str, value: str) -> None:
    raw = path.read_text(encoding="utf-8") if path.exists() else ""
    lines = raw.splitlines()
    section_header = f"[{section}]"
    if not lines:
        lines = [section_header]
    section_start = None
    section_end = len(lines)
    for idx, line in enumerate(lines):
        if line.strip() == section_header:
            section_start = idx
            break
    if section_start is None:
        if lines and lines[-1].strip() != "":
            lines.append("")
        lines.append(section_header)
        section_start = len(lines) - 1
        section_end = len(lines)
    else:
        for idx in range(section_start + 1, len(lines)):
            stripped = lines[idx].strip()
            if stripped.startswith("[") and stripped.endswith("]"):
                section_end = idx
                break
    key_line_prefix = f"{key} ="
    for idx in range(section_start + 1, section_end):
        if lines[idx].strip().startswith(key_line_prefix):
            lines[idx] = f'{key} = "{value}"'
            path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            return
    lines.insert(section_end, f'{key} = "{value}"')
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def bootstrap_firebase_auth_if_needed(args: argparse.Namespace, cfg: dict[str, Any]) -> dict[str, Any]:
    firebase_enabled = bool(nested_get(cfg, ("outputs", "firebase", "enabled"), False))
    if not firebase_enabled:
        return cfg

    service_account = str(nested_get(cfg, ("outputs", "firebase", "service_account"), "")).strip()
    auth_token = str(nested_get(cfg, ("outputs", "firebase", "auth_token"), "")).strip()
    api_key = str(nested_get(cfg, ("outputs", "firebase", "api_key"), "")).strip()
    refresh_token = str(nested_get(cfg, ("outputs", "firebase", "refresh_token"), "")).strip()
    has_refresh_mode = bool(api_key and refresh_token)
    if service_account or auth_token or has_refresh_mode:
        return cfg

    print("[orecchio] firebase auth is incomplete; starting interactive login bootstrap")
    if not api_key:
        api_key = input("Firebase Web API key: ").strip()
    email = input("Firebase Auth email: ").strip()
    password = getpass.getpass("Firebase Auth password: ")
    auth = firebase_sign_in_with_password(api_key=api_key, email=email, password=password)
    new_refresh = str(auth.get("refreshToken", "")).strip()
    new_id = str(auth.get("idToken", "")).strip()
    if not new_refresh:
        raise RuntimeError("firebase bootstrap failed: no refreshToken returned")

    config_path = Path(args.config)
    upsert_toml_key_in_section(config_path, "outputs.firebase", "api_key", api_key)
    upsert_toml_key_in_section(config_path, "outputs.firebase", "refresh_token", new_refresh)
    upsert_toml_key_in_section(config_path, "outputs.firebase", "id_token", new_id)
    upsert_toml_key_in_section(config_path, "outputs.firebase", "auth_token", "")
    upsert_toml_key_in_section(config_path, "outputs.firebase", "service_account", "")
    print(f"[orecchio] wrote Firebase refresh auth fields to {config_path}")

    nested_set(cfg, ("outputs", "firebase", "api_key"), api_key)
    nested_set(cfg, ("outputs", "firebase", "refresh_token"), new_refresh)
    nested_set(cfg, ("outputs", "firebase", "id_token"), new_id)
    nested_set(cfg, ("outputs", "firebase", "auth_token"), "")
    nested_set(cfg, ("outputs", "firebase", "service_account"), "")
    return cfg


def main() -> None:
    configure_stdio()
    parser = build_arg_parser()
    args = parser.parse_args()

    cfg = load_config(args)
    if args.command == "login":
        sys.exit(run_login_command(args, cfg))
    cfg = bootstrap_firebase_auth_if_needed(args, cfg)
    validate_config(cfg)

    site_id = str(nested_get(cfg, ("site", "id")))
    sample_rate_hz = int(nested_get(cfg, ("source", "sample_rate_hz")))
    read_chunk_seconds = float(nested_get(cfg, ("runtime", "read_chunk_seconds"), 0.5))
    reconnect_delay = float(nested_get(cfg, ("source", "reconnect_delay_seconds"), 3.0))
    dump_top = bool(nested_get(cfg, ("runtime", "dump_top"), False))
    drop_policy = str(nested_get(cfg, ("runtime", "drop_policy"), "drop_oldest_branch_job"))
    health_report_seconds = float(nested_get(cfg, ("runtime", "health_report_seconds"), 30.0))

    ring = RingBuffer(
        sample_rate_hz=sample_rate_hz,
        seconds=int(nested_get(cfg, ("runtime", "ring_buffer_seconds"), 900)),
    )
    branches = build_branches(cfg)
    metrics = IngestMetrics(
        report_interval_seconds=health_report_seconds,
        start_monotonic=time.monotonic(),
        next_report_monotonic=time.monotonic() + health_report_seconds,
    )

    print(
        f"[{iso(utc_now())}] orecchio starting site={site_id} "
        f"provider={nested_get(cfg, ('source', 'provider'))} "
        f"branches={','.join([b.name for b in branches]) or 'none'}"
    )

    ffmpeg = start_ffmpeg(cfg)
    if ffmpeg.stdout is None:
        raise RuntimeError("ffmpeg stdout unavailable")

    running = True

    def shutdown(*_: Any) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    chunk_samples = max(1, int(round(sample_rate_hz * read_chunk_seconds)))
    chunk_bytes = chunk_samples * BYTES_PER_SAMPLE

    try:
        while running:
            raw = ffmpeg.stdout.read(chunk_bytes)
            if not raw:
                err = read_ffmpeg_stderr(ffmpeg)
                print(f"[{iso(utc_now())}] ffmpeg stream ended; reconnecting. stderr:\n{err}", file=sys.stderr)
                stop_ffmpeg(ffmpeg)
                metrics.reconnects += 1

                while running:
                    time.sleep(reconnect_delay)
                    ffmpeg = start_ffmpeg(cfg)
                    if ffmpeg.stdout is None:
                        print(f"[{iso(utc_now())}] ffmpeg restart failed (stdout unavailable), retrying...", file=sys.stderr)
                        stop_ffmpeg(ffmpeg)
                        continue
                    print(f"[{iso(utc_now())}] ffmpeg reconnected")
                    break
                continue

            samples = pcm16_to_float(raw)
            overwritten = ring.append(samples)
            metrics.record_chunk(samples.size, overwritten)

            now_utc = utc_now()
            now_mono = time.monotonic()
            for branch in branches:
                branch.enqueue_due(now_monotonic=now_mono, drop_policy=drop_policy)
            for branch in branches:
                branch.run_one(
                    ring=ring,
                    canonical_sample_rate_hz=sample_rate_hz,
                    now_monotonic=now_mono,
                    now_utc=now_utc,
                    dump_top=dump_top,
                )

            if metrics.should_report(now_mono):
                print(f"[{iso(now_utc)}] {metrics.render(now_mono)}")
                for branch in branches:
                    print(f"[{iso(now_utc)}] {branch.health_line()}")
                metrics.advance_report_window(now_mono)
    finally:
        for branch in branches:
            finalize = getattr(branch, "finalize", None)
            if callable(finalize):
                finalize()
        stop_ffmpeg(ffmpeg)
        print(f"[{iso(utc_now())}] orecchio stopped")


if __name__ == "__main__":
    main()
