#!/bin/sh
set -e

python3 scripts/generate_demo_data.py
python3 scripts/inject_anomalies.py
python3 scripts/run_batch_pipeline.py
python3 scripts/run_streaming_pipeline.py
python3 dashboard/app.py
python3 -m http.server 8000 -d dashboard
