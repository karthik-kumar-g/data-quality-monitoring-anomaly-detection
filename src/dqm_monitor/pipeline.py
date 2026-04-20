"""Batch and streaming pipelines for local data quality monitoring."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from dqm_monitor.config import (
    BATCH_INPUT_DIR,
    BATCH_MANIFEST_PATH,
    FEATURE_COLUMNS,
    RESULTS_DIR,
    STREAM_INPUT_DIR,
    STREAM_MANIFEST_PATH,
    TRAINING_BATCHES,
)
from dqm_monitor.detectors import evaluate_all_detectors
from dqm_monitor.quality_checks import align_to_expected_schema, build_baseline_stats, compute_quality_metrics
from dqm_monitor.utils import ensure_directories, numeric_batch_id, write_text


def _build_reference_frames(asset_paths: list[Path], count: int) -> list[pd.DataFrame]:
    reference_frames: list[pd.DataFrame] = []
    for path in sorted(asset_paths)[:count]:
        df = pd.read_csv(path)
        aligned, _ = align_to_expected_schema(df)
        reference_frames.append(aligned)
    return reference_frames


def _save_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def run_batch_pipeline(
    input_dir: Path = BATCH_INPUT_DIR,
    manifest_path: Path = BATCH_MANIFEST_PATH,
    results_dir: Path = RESULTS_DIR / "batch",
) -> dict[str, Path]:
    ensure_directories([results_dir, RESULTS_DIR / "logs"])
    asset_paths = sorted(input_dir.glob("batch_*.csv"))
    baseline_stats = build_baseline_stats(_build_reference_frames(asset_paths, TRAINING_BATCHES))
    metric_rows: list[dict[str, object]] = []

    for path in asset_paths:
        df = pd.read_csv(path)
        metrics = compute_quality_metrics(df, path.name, baseline_stats)
        metrics["batch_index"] = numeric_batch_id(path.name)
        metric_rows.append(metrics)

    metrics_df = pd.DataFrame(metric_rows).sort_values("batch_index")
    manifest_df = pd.read_csv(manifest_path)
    merged_df = metrics_df.merge(manifest_df, left_on=["asset_name", "batch_index"], right_on=["batch_name", "batch_index"])
    feature_frame = merged_df[FEATURE_COLUMNS].fillna(0.0)
    labels = merged_df["is_anomalous"].astype(int)
    train_mask = merged_df["batch_index"] <= TRAINING_BATCHES
    detector_predictions, detector_summary = evaluate_all_detectors(feature_frame, labels, train_mask)

    full_output = pd.concat([merged_df.reset_index(drop=True), detector_predictions], axis=1)
    quality_path = results_dir / "quality_metrics.csv"
    detector_path = results_dir / "detector_summary.csv"
    full_output.to_csv(quality_path, index=False)
    detector_summary.to_csv(detector_path, index=False)

    summary_payload = {
        "pipeline": "batch",
        "windows_processed": int(len(full_output)),
        "anomalous_windows": int(labels.sum()),
        "best_detector": detector_summary.iloc[0]["detector"],
        "best_f1_score": float(detector_summary.iloc[0]["f1_score"]),
        "semester_scope": "Semester 1 prototype",
    }
    _save_json(results_dir / "summary.json", summary_payload)
    write_text(
        RESULTS_DIR / "logs" / "batch_pipeline.log",
        "\n".join(
            [
                "Batch pipeline executed successfully.",
                f"Processed windows: {summary_payload['windows_processed']}",
                f"Detected anomalies in manifest: {summary_payload['anomalous_windows']}",
                f"Best detector by F1: {summary_payload['best_detector']}",
            ]
        ),
    )
    return {
        "quality_metrics": quality_path,
        "detector_summary": detector_path,
        "summary": results_dir / "summary.json",
    }


def run_stream_pipeline(
    input_dir: Path = STREAM_INPUT_DIR,
    manifest_path: Path = STREAM_MANIFEST_PATH,
    results_dir: Path = RESULTS_DIR / "stream",
    max_chunks: int | None = None,
) -> dict[str, Path]:
    ensure_directories([results_dir, RESULTS_DIR / "logs"])
    asset_paths = sorted(input_dir.glob("*.csv"))
    if max_chunks is not None:
        asset_paths = asset_paths[:max_chunks]

    baseline_stats = build_baseline_stats(_build_reference_frames(asset_paths, min(TRAINING_BATCHES * 2, len(asset_paths))))
    metric_rows: list[dict[str, object]] = []

    for path in asset_paths:
        df = pd.read_csv(path)
        metrics = compute_quality_metrics(df, path.name, baseline_stats)
        metrics["chunk_name"] = path.name
        metrics["batch_index"] = numeric_batch_id(path.name)
        metric_rows.append(metrics)

    metrics_df = pd.DataFrame(metric_rows).sort_values(["batch_index", "chunk_name"])
    manifest_df = pd.read_csv(manifest_path)
    merged_df = metrics_df.merge(manifest_df, on=["chunk_name", "batch_index"], how="left")
    feature_frame = merged_df[FEATURE_COLUMNS].fillna(0.0)
    labels = merged_df["is_anomalous"].astype(int)
    train_mask = merged_df["batch_index"] <= TRAINING_BATCHES
    detector_predictions, detector_summary = evaluate_all_detectors(feature_frame, labels, train_mask)

    full_output = pd.concat([merged_df.reset_index(drop=True), detector_predictions], axis=1)
    quality_path = results_dir / "stream_quality_metrics.csv"
    detector_path = results_dir / "stream_detector_summary.csv"
    full_output.to_csv(quality_path, index=False)
    detector_summary.to_csv(detector_path, index=False)

    summary_payload = {
        "pipeline": "stream",
        "chunks_processed": int(len(full_output)),
        "anomalous_chunks": int(labels.sum()),
        "best_detector": detector_summary.iloc[0]["detector"],
        "best_f1_score": float(detector_summary.iloc[0]["f1_score"]),
    }
    _save_json(results_dir / "summary.json", summary_payload)
    write_text(
        RESULTS_DIR / "logs" / "stream_pipeline.log",
        "\n".join(
            [
                "Streaming simulation executed successfully.",
                f"Processed chunks: {summary_payload['chunks_processed']}",
                f"Anomalous chunks: {summary_payload['anomalous_chunks']}",
                f"Best detector by F1: {summary_payload['best_detector']}",
            ]
        ),
    )
    return {
        "quality_metrics": quality_path,
        "detector_summary": detector_path,
        "summary": results_dir / "summary.json",
    }
