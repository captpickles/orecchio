# Orecchio Config Contract (Phase 0)

## Format

- Primary config file: `orecchio.toml`
- Python support: `tomllib` (built in for Python 3.11+, project target is 3.12+)

## Precedence

1. CLI flags (highest)
2. Environment variables
3. `orecchio.toml`
4. Built-in defaults (lowest)

## Key Naming

- CLI: `--site-id`, `--source-provider`, `--source-device`, etc.
- Env: `ORECCHIO_SITE_ID`, `ORECCHIO_SOURCE_PROVIDER`, `ORECCHIO_SOURCE_DEVICE`, etc.
- TOML: nested sections (example in `orecchio.toml.example`)

## Required Inputs

- `site.id`
- Source:
  - For `avfoundation`: `source.device`
  - For `rtsp`: `source.rtsp_url`

## Firebase Path Rule

- All writes are namespaced:
  - `<outputs.firebase.base_path>/<site.id>/events/...`
  - `<outputs.firebase.base_path>/<site.id>/daily_summary/...`
- Default `base_path` is `orecchio_sites` to stay distinct from legacy `ear.py` root paths.

## Firebase Auth Modes

- Admin SDK mode:
  - `outputs.firebase.service_account`
  - bypasses RTDB rules (trusted backend mode)
- REST static token mode:
  - `outputs.firebase.auth_token`
- REST refresh-token mode (recommended for shared-hosted UI + per-site rules):
  - `outputs.firebase.api_key`
  - `outputs.firebase.refresh_token`
  - optional: `outputs.firebase.id_token` (seed token)
  - ingestor auto-refreshes `idToken` and retries once on 401/403

## Branch Config Rule

- Each branch (`yamnet`, `birdnet`) must define:
  - `enabled`
  - `sample_rate_hz`
  - `window_seconds`
  - `hop_seconds`

## Runtime Control Keys

- `runtime.drop_policy`
  - `drop_oldest_branch_job` (default)
  - `drop_newest_branch_job`
- `runtime.health_report_seconds`
  - periodic ingest + branch health log interval

## BirdNET Easy Mode

- `birdnetlib` initializes BirdNET analyzer assets on first use.
- If `birdnet` branch is enabled and `birdnetlib` is missing, install with:
  - `pip install birdnetlib`

## BirdWeather Export Path

- Queue-first design:
  - detections are always written locally first
  - export candidates are appended to `outputs.birdweather.queue_jsonl`
- Manual-review gate:
  - `outputs.birdweather.require_manual_review = true` keeps entries pending review
  - when false, entries are moved to `outputs.birdweather.sent_jsonl` as auto-ready placeholders
- Firebase event payloads include `site_id`, `source_id`, and `model` tags.
- BirdNET detections are written to Firebase under:
  - `<outputs.firebase.base_path>/<site.id>/birds/<push-id>`

## First CLI Surface (planned)

- `--config orecchio.toml`
- `--site-id backyard-nyc-01`
- `--source-provider avfoundation`
- `--source-device ":USB Audio CODEC"`
- `--enable-branches yamnet,birdnet`
- `--firebase-enabled`
- `--firebase-db-url ...`
- `--dump-top`
