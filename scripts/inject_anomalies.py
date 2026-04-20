"""Create anomalous batch and stream inputs from the clean demo data."""

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dqm_monitor.anomaly_injection import create_anomalous_batches


if __name__ == "__main__":
    batch_manifest, stream_manifest = create_anomalous_batches()
    print(f"Prepared {len(batch_manifest)} batch files and {len(stream_manifest)} stream chunks.")
