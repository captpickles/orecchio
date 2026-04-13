
# current test-bed

.venv/bin/python ear.py \
--rtsp 'rtsps://192.168.1.134:7441/AFGng3qi9jYUUjEG?enableSrtp' \
--events-csv events.csv \
--chunk-seconds 2 \
--daily-csv daily_summary.csv \
--dump-top
