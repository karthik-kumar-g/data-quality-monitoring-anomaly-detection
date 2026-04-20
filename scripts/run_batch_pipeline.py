"""Execute the batch data quality pipeline."""

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dqm_monitor.pipeline import run_batch_pipeline


if __name__ == "__main__":
    outputs = run_batch_pipeline()
    print("Batch pipeline outputs:")
    for key, value in outputs.items():
        print(f"  {key}: {value}")
