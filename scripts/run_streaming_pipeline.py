"""Execute the streaming simulation pipeline."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dqm_monitor.pipeline import run_stream_pipeline


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the simulated streaming quality pipeline.")
    parser.add_argument("--max-chunks", type=int, default=None, help="Optional limit on processed chunks.")
    args = parser.parse_args()

    outputs = run_stream_pipeline(max_chunks=args.max_chunks)
    print("Streaming pipeline outputs:")
    for key, value in outputs.items():
        print(f"  {key}: {value}")
