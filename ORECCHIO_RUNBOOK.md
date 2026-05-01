# Orecchio Runbook

## Scope

This runbook covers shadow validation, cutover, bootstrap, and rollback for `orecchio.py`.
`ear.py` remains unchanged and can run in parallel.

## Bootstrap New Site

1. Copy config template:
   - `cp orecchio.toml.example orecchio.toml`
2. Set required values in `orecchio.toml`:
   - `site.id`
   - source:
     - `source.provider = "avfoundation"` + `source.device`, or
     - `source.provider = "rtsp"` + `source.rtsp_url`
3. Confirm local output paths are distinct:
   - `outputs.local.events_csv = "orecchio_events.csv"`
   - `outputs.local.daily_csv = "orecchio_daily_summary.csv"`
4. (Optional) Enable Firebase with isolated namespace:
   - `outputs.firebase.enabled = true`
   - `outputs.firebase.base_path = "orecchio_sites"`
5. (Optional) Enable BirdWeather queue:
   - `outputs.birdweather.enabled = true`
   - Keep `outputs.birdweather.require_manual_review = true` by default.

## Shadow Validation

Run `ear.py` and `orecchio.py` in parallel against the same source.

Example:

```bash
.venv/bin/python ear.py &
.venv/bin/python orecchio.py --config orecchio.toml &
```

Keep shadow mode long enough to observe representative daytime and nighttime conditions.

### Compare Outputs

Use the comparison tool:

```bash
.venv/bin/python scripts/shadow_compare.py \
  --legacy-events events.csv \
  --orecchio-events orecchio_events.csv
```

Optional bounded window:

```bash
.venv/bin/python scripts/shadow_compare.py \
  --start-utc 2026-04-27T12:00:00Z \
  --end-utc 2026-04-28T12:00:00Z
```

### Tune Loop

1. Inspect mismatches and duration drift.
2. Tune YAMNet thresholds/regime windows.
3. Repeat shadow run and compare until stable.

## Cutover Checklist

- [ ] `orecchio.py` shadow metrics acceptable.
- [ ] `orecchio_events.csv` parity acceptable vs `events.csv`.
- [ ] Firebase receiving under `orecchio_sites/<site-id>/...`.
- [ ] BirdNET detections writing to `orecchio_detections.jsonl`.
- [ ] BirdWeather queue behavior confirmed (`pending_review` by default).
- [ ] Monitoring/log stream reviewed for reconnect churn and lag.

Then:
1. Stop `ear.py`.
2. Keep `orecchio.py` as primary.
3. Watch first 24h for regressions.

## Rollback

If regression is detected:

1. Stop `orecchio.py`.
2. Restart `ear.py`.
3. Keep `orecchio` data for analysis:
   - `orecchio_events.csv`
   - `orecchio_daily_summary.csv`
   - `orecchio_detections.jsonl`
   - `orecchio_birdweather_queue.jsonl`
4. File threshold/config fixes and repeat shadow mode.

## Failure Modes

- `ffmpeg` source reconnect loops:
  - check device name / RTSP URL / network.
- branch lag growth:
  - reduce branch frequency (`hop_seconds`) or `top_k`, increase ring buffer.
- Firebase write errors:
  - verify `db_url`, auth, and `outputs.firebase.base_path`.
- BirdNET model init failure:
  - ensure `birdnetlib` installed and first-run model download can complete.
