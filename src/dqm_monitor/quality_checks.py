"""Data quality checks used by the batch and streaming pipelines."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from dqm_monitor.config import CRITICAL_COLUMNS, EXPECTED_SCHEMA, NUMERIC_BOUNDS


@dataclass
class SchemaInspection:
    missing_columns: list[str]
    extra_columns: list[str]

    @property
    def schema_drift_flag(self) -> int:
        return int(bool(self.missing_columns or self.extra_columns))


def inspect_schema(columns: list[str]) -> SchemaInspection:
    expected_set = set(EXPECTED_SCHEMA)
    actual_set = set(columns)
    return SchemaInspection(
        missing_columns=sorted(expected_set - actual_set),
        extra_columns=sorted(actual_set - expected_set),
    )


def align_to_expected_schema(df: pd.DataFrame) -> tuple[pd.DataFrame, SchemaInspection]:
    inspection = inspect_schema(list(df.columns))
    aligned = df.reindex(columns=EXPECTED_SCHEMA)
    aligned["event_ts"] = pd.to_datetime(aligned["event_ts"], format="mixed", errors="coerce")
    aligned["ingest_ts"] = pd.to_datetime(aligned["ingest_ts"], format="mixed", errors="coerce")
    return aligned, inspection


def _population_stability_index(reference: pd.Series, current: pd.Series, bins: int = 8) -> float:
    reference = reference.dropna().astype(float)
    current = current.dropna().astype(float)
    if reference.empty or current.empty:
        return 0.0
    quantiles = np.unique(np.quantile(reference, np.linspace(0, 1, bins + 1)))
    if len(quantiles) < 3:
        return 0.0
    ref_counts, _ = np.histogram(reference, bins=quantiles)
    cur_counts, _ = np.histogram(current, bins=quantiles)
    ref_ratio = np.where(ref_counts == 0, 1e-6, ref_counts / ref_counts.sum())
    cur_ratio = np.where(cur_counts == 0, 1e-6, cur_counts / cur_counts.sum())
    return float(np.sum((cur_ratio - ref_ratio) * np.log(cur_ratio / ref_ratio)))


def build_baseline_stats(reference_frames: list[pd.DataFrame]) -> dict[str, float | pd.Series]:
    combined = pd.concat(reference_frames, ignore_index=True)
    missing_rate = combined[CRITICAL_COLUMNS].isna().mean().mean()
    freshness_delay = (combined["ingest_ts"] - combined["event_ts"]).dt.total_seconds().div(60).dropna()
    return {
        "baseline_missing_rate": float(missing_rate),
        "baseline_row_count": float(np.mean([len(frame) for frame in reference_frames])),
        "baseline_freshness_delay_minutes": float(freshness_delay.mean()),
        "reference_amount_distribution": combined["amount"],
    }


def compute_quality_metrics(
    df: pd.DataFrame,
    asset_name: str,
    baseline_stats: dict[str, float | pd.Series],
) -> dict[str, object]:
    aligned, inspection = align_to_expected_schema(df)
    row_count = len(aligned)
    missing_rate = float(aligned[CRITICAL_COLUMNS].isna().mean().mean())
    duplicate_rate = float(aligned.duplicated(subset=["record_id"]).mean())
    out_of_range_mask = pd.Series(False, index=aligned.index)

    for column_name, (lower, upper) in NUMERIC_BOUNDS.items():
        column = pd.to_numeric(aligned[column_name], errors="coerce")
        out_of_range_mask = out_of_range_mask | column.lt(lower) | column.gt(upper)

    out_of_range_rate = float(out_of_range_mask.mean())
    freshness_delay = (aligned["ingest_ts"] - aligned["event_ts"]).dt.total_seconds().div(60)
    freshness_delay_mean_minutes = float(freshness_delay.mean()) if not freshness_delay.dropna().empty else 0.0
    delayed_record_rate = float(freshness_delay.gt(120).mean())
    distribution_drift_score = _population_stability_index(
        baseline_stats["reference_amount_distribution"], aligned["amount"]
    )
    row_count_delta = float((row_count - baseline_stats["baseline_row_count"]) / baseline_stats["baseline_row_count"])
    null_spike_score = float(max(0.0, missing_rate - baseline_stats["baseline_missing_rate"]))

    trigger_map = {
        "missing_values": missing_rate > 0.08,
        "duplicates": duplicate_rate > 0.03,
        "out_of_range": out_of_range_rate > 0.02,
        "freshness": freshness_delay_mean_minutes > 180 or delayed_record_rate > 0.20,
        "distribution_drift": distribution_drift_score > 0.15,
        "schema_drift": inspection.schema_drift_flag == 1,
        "null_spike": null_spike_score > 0.05,
        "row_count_shift": abs(row_count_delta) > 0.25,
    }
    active_triggers = [name for name, is_active in trigger_map.items() if is_active]

    return {
        "asset_name": asset_name,
        "row_count": row_count,
        "missing_rate": round(missing_rate, 4),
        "duplicate_rate": round(duplicate_rate, 4),
        "out_of_range_rate": round(out_of_range_rate, 4),
        "freshness_delay_mean_minutes": round(freshness_delay_mean_minutes, 2),
        "delayed_record_rate": round(delayed_record_rate, 4),
        "distribution_drift_score": round(distribution_drift_score, 4),
        "row_count_delta": round(row_count_delta, 4),
        "schema_drift_flag": inspection.schema_drift_flag,
        "missing_columns": ",".join(inspection.missing_columns) if inspection.missing_columns else "none",
        "extra_columns": ",".join(inspection.extra_columns) if inspection.extra_columns else "none",
        "null_spike_score": round(null_spike_score, 4),
        "rule_trigger_count": len(active_triggers),
        "triggered_rules": ",".join(active_triggers) if active_triggers else "none",
        "rule_based_anomaly": int(bool(active_triggers)),
    }
