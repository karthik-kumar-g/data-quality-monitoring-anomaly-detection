"""Project-wide configuration for the Semester 1 prototype."""

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
RESULTS_DIR = PROJECT_ROOT / "results"

BASE_BATCH_DIR = DATA_DIR / "base_batches"
BATCH_INPUT_DIR = DATA_DIR / "batch_inputs"
STREAM_INPUT_DIR = DATA_DIR / "stream_inputs"
BATCH_MANIFEST_PATH = DATA_DIR / "batch_manifest.csv"
STREAM_MANIFEST_PATH = DATA_DIR / "stream_manifest.csv"

EXPECTED_SCHEMA = [
    "record_id",
    "batch_id",
    "event_ts",
    "ingest_ts",
    "source_system",
    "region",
    "customer_id",
    "product_id",
    "status",
    "quantity",
    "amount",
]

CRITICAL_COLUMNS = ["record_id", "event_ts", "ingest_ts", "customer_id", "product_id", "amount"]

NUMERIC_BOUNDS = {
    "quantity": (1, 10),
    "amount": (0.0, 5000.0),
}

TRAINING_BATCHES = 6
STREAM_CHUNK_SIZE = 120
RANDOM_SEED = 42

FEATURE_COLUMNS = [
    "missing_rate",
    "duplicate_rate",
    "out_of_range_rate",
    "freshness_delay_mean_minutes",
    "delayed_record_rate",
    "distribution_drift_score",
    "row_count_delta",
    "schema_drift_flag",
    "null_spike_score",
    "rule_trigger_count",
]

ANOMALY_PLAN = {
    "batch_007.csv": ["missing_values", "null_spike"],
    "batch_009.csv": ["duplicates"],
    "batch_011.csv": ["out_of_range"],
    "batch_013.csv": ["freshness_delay"],
    "batch_015.csv": ["distribution_drift"],
    "batch_016.csv": ["schema_drift"],
}
