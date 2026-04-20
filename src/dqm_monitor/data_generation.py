"""Synthetic data generation for batch inputs used in Semester 1."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from dqm_monitor.config import BASE_BATCH_DIR, RANDOM_SEED
from dqm_monitor.utils import ensure_directories


def generate_clean_batches(
    output_dir: Path = BASE_BATCH_DIR,
    num_batches: int = 16,
    rows_per_batch: int = 320,
    seed: int = RANDOM_SEED,
) -> list[Path]:
    """Generate clean synthetic batches with stable distributions."""
    ensure_directories([output_dir])
    rng = np.random.default_rng(seed)
    start_date = datetime(2026, 1, 1, 8, 0, 0)
    generated_files: list[Path] = []
    running_record_id = 0

    for batch_number in range(1, num_batches + 1):
        batch_id = f"batch_{batch_number:03d}"
        batch_start = start_date + timedelta(days=batch_number - 1)
        regions = rng.choice(["north", "south", "east", "west"], size=rows_per_batch, p=[0.28, 0.22, 0.26, 0.24])
        sources = rng.choice(["erp", "crm", "mobile_app"], size=rows_per_batch, p=[0.45, 0.3, 0.25])
        statuses = rng.choice(["completed", "pending", "cancelled"], size=rows_per_batch, p=[0.84, 0.11, 0.05])
        quantities = rng.poisson(lam=3.2, size=rows_per_batch) + 1
        quantities = np.clip(quantities, 1, 8)
        base_price = rng.normal(loc=145.0, scale=18.0, size=rows_per_batch)
        regional_multiplier = np.select(
            [regions == "north", regions == "south", regions == "east", regions == "west"],
            [1.08, 0.94, 1.02, 1.0],
            default=1.0,
        )
        amounts = quantities * base_price * regional_multiplier + rng.normal(loc=0.0, scale=12.0, size=rows_per_batch)
        amounts = np.clip(amounts, 25.0, None).round(2)
        event_offsets = rng.integers(low=0, high=24 * 60, size=rows_per_batch)
        delay_minutes = np.clip(rng.normal(loc=28.0, scale=11.0, size=rows_per_batch), 5, None)

        records = []
        for row_number in range(rows_per_batch):
            running_record_id += 1
            event_ts = batch_start + timedelta(minutes=int(event_offsets[row_number]))
            ingest_ts = event_ts + timedelta(minutes=float(delay_minutes[row_number]))
            records.append(
                {
                    "record_id": f"RID{running_record_id:07d}",
                    "batch_id": batch_id,
                    "event_ts": event_ts.isoformat(),
                    "ingest_ts": ingest_ts.isoformat(),
                    "source_system": sources[row_number],
                    "region": regions[row_number],
                    "customer_id": f"CUST{rng.integers(1000, 9999)}",
                    "product_id": f"PROD{rng.integers(10, 250)}",
                    "status": statuses[row_number],
                    "quantity": int(quantities[row_number]),
                    "amount": float(amounts[row_number]),
                }
            )

        batch_df = pd.DataFrame.from_records(records)
        batch_path = output_dir / f"{batch_id}.csv"
        batch_df.to_csv(batch_path, index=False)
        generated_files.append(batch_path)

    return generated_files
