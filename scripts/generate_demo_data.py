"""Generate clean Semester 1 demo data."""

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dqm_monitor.data_generation import generate_clean_batches


if __name__ == "__main__":
    generated = generate_clean_batches()
    print(f"Generated {len(generated)} clean batches in {PROJECT_ROOT / 'data' / 'base_batches'}")
