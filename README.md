
# orecch.io baseline detector

Small local baseline for passive acoustic event detection from a UniFi camera stream.

## What it does

- Reads audio from RTSP/RTSPS via `ffmpeg`
- Resamples to mono 16 kHz PCM
- Runs YAMNet on fixed chunks
- Aggregates chunk labels into higher-level events
- Writes:
- `events.csv`
- `daily_summary.csv`
- Optionally sends one-line Slack notifications per closed event

## Prerequisites

- macOS (or Linux)
- Python 3.12+
- `ffmpeg` installed (macOS: `brew install ffmpeg`)

## Local setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Do not commit `.venv` (already ignored in `.gitignore`).

## Run

```bash
.venv/bin/python ear.py \
  --rtsp 'rtsps://<camera-ip>:7441/<stream-token>?enableSrtp' \
  --events-csv events.csv \
  --daily-csv daily_summary.csv \
  --chunk-seconds 2 \
  --dump-top
```

With Slack:

```bash
.venv/bin/python ear.py \
  --rtsp 'rtsps://<camera-ip>:7441/<stream-token>?enableSrtp' \
  --events-csv events.csv \
  --daily-csv daily_summary.csv \
  --chunk-seconds 2 \
  --slack-webhook 'https://hooks.slack.com/services/...' \
  --dump-top
```

## Output files

- `events.csv`: one row per closed event
- `daily_summary.csv`: per-day per-event counts and durations

## UniFi camera RTSP/RTSPS setup (suggested)

UI labels vary by UniFi Protect version, but the flow is usually:

1. Open UniFi Protect.
2. Select camera.
3. Find stream settings / advanced settings.
4. Enable RTSP or RTSPS stream.
5. Copy the generated stream URL/token.

Notes:

- Prefer `rtsps://` when available.
- Keep camera and detector on the same LAN/VPN.
- Verify audio exists before tuning detection:
- Record a short clip with `ffmpeg`.
- Listen to confirm your target sounds are actually present.

Quick audio check:

```bash
ffmpeg -hide_banner -loglevel error \
  -rtsp_transport tcp \
  -i 'rtsps://<camera-ip>:7441/<stream-token>?enableSrtp' \
  -t 20 -vn -ac 1 -ar 16000 -c:a pcm_s16le check.wav
```
