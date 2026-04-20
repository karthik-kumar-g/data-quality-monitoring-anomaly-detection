# Data Quality Monitoring and Anomaly Detection in Large-Scale Data Pipelines

Lightweight Semester 1 prototype for monitoring data quality issues in recurring data pipelines using fully local, open-source tooling.

## Overview

This project simulates a recurring batch and streaming-style data pipeline, injects controlled data quality anomalies, computes rule-based quality metrics, compares lightweight anomaly detectors, and generates a local HTML dashboard for inspection.

The repository is intentionally kept small and deployment-focused:
- no cloud dependency
- no academic submission files
- no generated outputs committed to Git
- runnable on a laptop or lab machine

## What The Project Detects

- missing values
- null spikes
- duplicates
- out-of-range values
- schema drift
- distribution drift
- delayed or stale records
- anomalous batch or stream chunks based on quality feature scoring

## Tech Stack

- Python
- NumPy
- pandas
- Docker Compose
- static HTML dashboard generated from Python

## Repository Structure

```text
.
├── dashboard/
│   └── app.py
├── data/
│   └── .gitkeep
├── deployment/
│   ├── Dockerfile
│   └── start.sh
├── results/
│   └── .gitkeep
├── scripts/
│   ├── generate_demo_data.py
│   ├── inject_anomalies.py
│   ├── run_batch_pipeline.py
│   └── run_streaming_pipeline.py
├── src/dqm_monitor/
│   ├── anomaly_injection.py
│   ├── config.py
│   ├── data_generation.py
│   ├── detectors.py
│   ├── pipeline.py
│   ├── quality_checks.py
│   └── utils.py
├── .env.example
├── .gitignore
├── Makefile
├── docker-compose.yml
└── requirements.txt
```

## How It Works

1. Clean synthetic batch files are generated.
2. Controlled anomalies are injected into selected batches.
3. Batch files are split into stream-like chunks.
4. Rule-based quality checks compute metrics for each batch and chunk.
5. Three lightweight detectors score the feature vectors:
   - statistical thresholding
   - robust z-score
   - PCA reconstruction error
6. Results are written to `results/`.
7. A static dashboard is generated at `dashboard/index.html`.

## Quick Start

### Option 1: Local Python

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
make demo
make serve
```

Open [http://localhost:8000/index.html](http://localhost:8000/index.html)

### Option 2: Manual Commands

```bash
python3 scripts/generate_demo_data.py
python3 scripts/inject_anomalies.py
python3 scripts/run_batch_pipeline.py
python3 scripts/run_streaming_pipeline.py
python3 dashboard/app.py
python3 -m http.server 8000 -d dashboard
```

### Option 3: Docker Compose

```bash
cp .env.example .env
docker compose up --build
```

The container will:
- generate demo data
- inject anomalies
- run the batch pipeline
- run the streaming simulation
- build the dashboard
- serve the dashboard on port `8000`

## Common Commands

Run the full local demo:

```bash
make demo
```

Serve the dashboard:

```bash
make serve
```

Clean all generated outputs:

```bash
make clean
```

## Generated Outputs

These files are created only at runtime and are ignored by Git:

- `data/base_batches/`
- `data/batch_inputs/`
- `data/stream_inputs/`
- `data/batch_manifest.csv`
- `data/stream_manifest.csv`
- `results/batch/`
- `results/stream/`
- `results/logs/`
- `dashboard/index.html`

## Deployment Notes

- The repository tracks only source and deployment files.
- `data/` and `results/` stay empty in Git and are populated when the project runs.
- `.env.example` contains the default dashboard port and metadata values used for Docker deployment.

## Troubleshooting

If port `8000` is already in use:

```bash
python3 -m http.server 8001 -d dashboard
```

If Docker is running an older container:

```bash
docker compose down
docker compose up --build
```

If generated outputs need to be rebuilt from scratch:

```bash
make clean
make demo
```

## Semester 1 Scope

This repository covers the implementation baseline for Semester 1:
- local synthetic pipeline simulation
- anomaly injection and labeling
- batch and streaming-style monitoring
- detector comparison
- local dashboard generation
- deployment-ready packaging

Larger-scale optimization, richer alerting, stronger explainability, and more advanced detectors are intentionally deferred beyond this baseline.
