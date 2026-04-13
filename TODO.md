# orecch.io baseline TODO

## Keep baseline simple; improve correctness/reliability next

- Fix event duration accounting so single-hit events are not `0.0s` and total durations are not undercounted by about one chunk.
- Handle partial `ffmpeg.stdout.read()` chunks with a byte buffer; only run YAMNet on full chunk sizes.
- Allow multiple event types per chunk (not only one global best match) to avoid dropping overlapping sounds.
- Normalize rule label matching (lowercase explicit labels once; remove brittle exact-string assumptions).
- Validate CLI inputs (at least `--chunk-seconds > 0`) and fail with clear errors.
- Make `events.csv` header logic robust for existing-but-empty files.
- Improve stream lifecycle handling:
- Add `-nostdin` to ffmpeg args.
- Detect child exit with `poll()` and surface stderr context.
- Add minimal reconnect/backoff behavior on RTSP drop.
- Ensure shutdown path always flushes events and waits/cleans up ffmpeg process cleanly.
- Reduce heavy `daily_summary.csv` rewrites (batch or periodic flush) while preserving inspectable CSV output.
