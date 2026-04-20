"""Inject controlled anomalies into synthetic batches for evaluation."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from dqm_monitor.config import (
    ANOMALY_PLAN,
    BASE_BATCH_DIR,
    BATCH_INPUT_DIR,
    BATCH_MANIFEST_PATH,
    RANDOM_SEED,
    STREAM_CHUNK_SIZE,
    STREAM_INPUT_DIR,
    STREAM_MANIFEST_PATH,
)
from dqm_monitor.utils import ensure_directories, numeric_batch_id


def _inject_missing_values(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    df = df.copy()
    indices = rng.choice(df.index, size=max(12, len(df) // 8), replace=False)
    df.loc[indices, ["customer_id", "product_id", "amount"]] = np.nan
    return df


def _inject_null_spike(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    df = df.copy()
    indices = rng.choice(df.index, size=max(20, len(df) // 5), replace=False)
    df.loc[indices, ["status", "customer_id"]] = np.nan
    return df


def _inject_duplicates(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    duplicate_rows = df.sample(n=max(15, len(df) // 10), random_state=int(rng.integers(1, 10_000)))
    return pd.concat([df, duplicate_rows], ignore_index=True)


def _inject_out_of_range(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    df = df.copy()
    indices = rng.choice(df.index, size=max(12, len(df) // 9), replace=False)
    df.loc[indices[: len(indices) // 2], "quantity"] = 0
    df.loc[indices[len(indices) // 2 :], "amount"] = 9200.0
    return df


def _inject_freshness_delay(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    df = df.copy()
    delayed_indices = rng.choice(df.index, size=max(40, len(df) // 3), replace=False)
    ingest_ts = pd.to_datetime(df["ingest_ts"], format="mixed", errors="coerce")
    ingest_ts.loc[delayed_indices] = ingest_ts.loc[delayed_indices] + pd.to_timedelta(8, unit="h")
    df["ingest_ts"] = ingest_ts.dt.strftime("%Y-%m-%dT%H:%M:%S")
    return df


def _inject_distribution_drift(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    df = df.copy()
    drift_indices = rng.choice(df.index, size=max(45, len(df) // 3), replace=False)
    df.loc[drift_indices, "amount"] = (df.loc[drift_indices, "amount"] * 2.4).round(2)
    df.loc[drift_indices, "region"] = "north"
    return df


def _inject_schema_drift(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.drop(columns=["status"])
    df["status_code"] = "legacy"
    return df


INJECTION_HANDLERS = {
    "missing_values": _inject_missing_values,
    "null_spike": _inject_null_spike,
    "duplicates": _inject_duplicates,
    "out_of_range": _inject_out_of_range,
    "freshness_delay": _inject_freshness_delay,
    "distribution_drift": _inject_distribution_drift,
    "schema_drift": _inject_schema_drift,
}


def create_anomalous_batches(
    base_dir: Path = BASE_BATCH_DIR,
    batch_output_dir: Path = BATCH_INPUT_DIR,
    stream_output_dir: Path = STREAM_INPUT_DIR,
    seed: int = RANDOM_SEED,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build anomalous batch inputs and streaming chunks from clean sources."""
    ensure_directories([batch_output_dir, stream_output_dir])
    rng = np.random.default_rng(seed)
    batch_manifest_rows: list[dict[str, object]] = []
    stream_manifest_rows: list[dict[str, object]] = []

    for stream_file in stream_output_dir.glob("*.csv"):
        stream_file.unlink()
    for batch_file in batch_output_dir.glob("*.csv"):
        batch_file.unlink()

    for batch_path in sorted(base_dir.glob("batch_*.csv")):
        df = pd.read_csv(batch_path)
        anomaly_types = ANOMALY_PLAN.get(batch_path.name, [])
        for anomaly_type in anomaly_types:
            handler = INJECTION_HANDLERS[anomaly_type]
            if anomaly_type == "schema_drift":
                df = handler(df)
            else:
                df = handler(df, rng)

        output_path = batch_output_dir / batch_path.name
        df.to_csv(output_path, index=False)
        batch_index = numeric_batch_id(batch_path.name)
        batch_manifest_rows.append(
            {
                "batch_name": batch_path.name,
                "batch_index": batch_index,
                "is_anomalous": int(bool(anomaly_types)),
                "anomaly_types": ",".join(anomaly_types) if anomaly_types else "none",
                "semester_scope": "Semester 1",
            }
        )

        chunk_count = max(1, int(np.ceil(len(df) / STREAM_CHUNK_SIZE)))
        for chunk_index in range(chunk_count):
            chunk = df.iloc[chunk_index * STREAM_CHUNK_SIZE : (chunk_index + 1) * STREAM_CHUNK_SIZE].copy()
            chunk_name = f"{Path(batch_path.name).stem}_chunk_{chunk_index + 1:02d}.csv"
            chunk.to_csv(stream_output_dir / chunk_name, index=False)
            stream_manifest_rows.append(
                {
                    "chunk_name": chunk_name,
                    "batch_name": batch_path.name,
                    "batch_index": batch_index,
                    "is_anomalous": int(bool(anomaly_types)),
                    "anomaly_types": ",".join(anomaly_types) if anomaly_types else "none",
                }
            )

    batch_manifest = pd.DataFrame(batch_manifest_rows).sort_values("batch_index")
    stream_manifest = pd.DataFrame(stream_manifest_rows).sort_values(["batch_index", "chunk_name"])
    batch_manifest.to_csv(BATCH_MANIFEST_PATH, index=False)
    stream_manifest.to_csv(STREAM_MANIFEST_PATH, index=False)
    return batch_manifest, stream_manifest
