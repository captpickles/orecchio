
# orecchio

Audio ingest platform for environmental event detection.

`ear.py` is legacy and unchanged.  
`orecchio.py` is the active platform entrypoint.

## What `orecchio.py` does

- Ingests audio from `avfoundation` or `rtsp/rtsps` via `ffmpeg`
- Fans out one stream to multiple branches
- Runs:
  - `yamnet` event detection
  - `birdnet` detections
- Writes local outputs:
  - `orecchio_events.csv`
  - `orecchio_daily_summary.csv`
  - `orecchio_detections.jsonl`
- Optional Firebase writes under site namespace:
  - `orecchio_sites/<site_id>/events`
  - `orecchio_sites/<site_id>/daily_summary`
  - `orecchio_sites/<site_id>/birds`

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp orecchio.toml.example orecchio.toml
```

## Run

```bash
.venv/bin/python orecchio.py
```

`orecchio.py` reads `orecchio.toml` by default.

## Config

Main config file: `orecchio.toml`  
Example: `orecchio.toml.example`

Required minimum:
- `site.id`
- `source.provider`
- if `provider = "avfoundation"`: `source.device`
- if `provider = "rtsp"`: `source.rtsp_url`

## Firebase auth modes

In `[outputs.firebase]`, choose one mode:

1. Admin SDK (trusted, bypasses RTDB rules):
- `service_account`

2. REST static token:
- `auth_token`

3. REST refresh-token mode (recommended for friend installs):
- `api_key`
- `refresh_token`
- optional `id_token`

If Firebase is enabled and auth is incomplete, `python orecchio.py` now prompts for Firebase login and writes refresh auth fields back to `orecchio.toml`.

Manual token bootstrap command:

```bash
.venv/bin/python orecchio.py login \
  --firebase-api-key 'YOUR_API_KEY' \
  --firebase-email 'user@example.com' \
  --firebase-password 'password'
```

## RTDB rules for per-site writes (UID allowlist)

Use rules that gate writes by:
- `site_writers/<site_id>/<auth.uid> == true`

Set allowlist entries with:

```bash
.venv/bin/python scripts/site_writer_auth.py \
  --service-account /path/to/firebase-account.json \
  --db-url https://<project>-default-rtdb.firebaseio.com \
  --site-id wytheville-01 \
  --uid FIREBASE_UID_HERE
```

## eBird taxonomy

Bird detections include `ebird_species_code` using:
- direct BirdNET fields when available
- fallback mapping from `branches.birdnet.ebird_taxonomy_csv` (official eBird taxonomy CSV)

Default config path:
- `ebird_taxonomy_csv = "ebird_taxonomy.csv"`

## Docker

`Dockerfile` is included.

Recommended for friend deployments:
- mount `orecchio.toml` as `rw` for first auth bootstrap
- mount output data directory for persistence
- after bootstrap, config mount can be switched to `ro` if desired

## Additional docs

- `ORECCHIO_CONFIG.md` — config contract and auth modes
- `ORECCHIO_RUNBOOK.md` — shadow/cutover/rollback steps
- `UI_HANDOFF.md` — dashboard schema/path handoff
