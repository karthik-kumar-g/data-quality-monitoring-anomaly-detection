"""Lightweight anomaly detectors implemented with pandas and numpy only."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class StatisticalThresholdDetector:
    threshold: float = 2.75
    mean_: pd.Series | None = None
    std_: pd.Series | None = None

    def fit(self, train_features: pd.DataFrame) -> "StatisticalThresholdDetector":
        self.mean_ = train_features.mean()
        self.std_ = train_features.std().replace(0, 1e-6)
        return self

    def score_samples(self, features: pd.DataFrame) -> np.ndarray:
        if self.mean_ is None or self.std_ is None:
            raise ValueError("StatisticalThresholdDetector must be fitted first.")
        z_scores = ((features - self.mean_) / self.std_).clip(lower=0)
        return z_scores.mean(axis=1).to_numpy()

    def predict(self, features: pd.DataFrame) -> np.ndarray:
        return (self.score_samples(features) > self.threshold).astype(int)


@dataclass
class RobustZScoreDetector:
    threshold: float = 6.0
    median_: pd.Series | None = None
    mad_: pd.Series | None = None

    def fit(self, train_features: pd.DataFrame) -> "RobustZScoreDetector":
        self.median_ = train_features.median()
        mad = (train_features - self.median_).abs().median()
        self.mad_ = mad.replace(0, 1e-6)
        return self

    def score_samples(self, features: pd.DataFrame) -> np.ndarray:
        if self.median_ is None or self.mad_ is None:
            raise ValueError("RobustZScoreDetector must be fitted first.")
        robust_scores = 0.6745 * (features - self.median_) / self.mad_
        return robust_scores.abs().mean(axis=1).to_numpy()

    def predict(self, features: pd.DataFrame) -> np.ndarray:
        return (self.score_samples(features) > self.threshold).astype(int)


@dataclass
class PCAReconstructionDetector:
    n_components: int = 2
    threshold_scale: float = 3.0
    mean_: np.ndarray | None = None
    std_: np.ndarray | None = None
    components_: np.ndarray | None = None
    threshold_: float | None = None

    def fit(self, train_features: pd.DataFrame) -> "PCAReconstructionDetector":
        matrix = train_features.to_numpy(dtype=float)
        self.mean_ = matrix.mean(axis=0)
        self.std_ = matrix.std(axis=0)
        self.std_[self.std_ == 0] = 1e-6
        standardized = (matrix - self.mean_) / self.std_
        _, _, vh = np.linalg.svd(standardized, full_matrices=False)
        component_count = min(self.n_components, vh.shape[0])
        self.components_ = vh[:component_count]
        errors = self.score_samples(train_features)
        self.threshold_ = float(errors.mean() + self.threshold_scale * errors.std())
        return self

    def score_samples(self, features: pd.DataFrame) -> np.ndarray:
        if self.mean_ is None or self.std_ is None or self.components_ is None:
            raise ValueError("PCAReconstructionDetector must be fitted first.")
        matrix = features.to_numpy(dtype=float)
        standardized = (matrix - self.mean_) / self.std_
        projected = standardized @ self.components_.T
        reconstructed = projected @ self.components_
        residual = standardized - reconstructed
        return np.sqrt((residual**2).sum(axis=1))

    def predict(self, features: pd.DataFrame) -> np.ndarray:
        if self.threshold_ is None:
            raise ValueError("PCAReconstructionDetector must be fitted first.")
        return (self.score_samples(features) > self.threshold_).astype(int)


def _classification_summary(labels: pd.Series, predictions: np.ndarray) -> dict[str, float]:
    label_array = labels.to_numpy(dtype=int)
    prediction_array = predictions.astype(int)
    tp = int(((label_array == 1) & (prediction_array == 1)).sum())
    tn = int(((label_array == 0) & (prediction_array == 0)).sum())
    fp = int(((label_array == 0) & (prediction_array == 1)).sum())
    fn = int(((label_array == 1) & (prediction_array == 0)).sum())

    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1_score = (2 * precision * recall / (precision + recall)) if precision + recall else 0.0
    accuracy = (tp + tn) / len(label_array) if len(label_array) else 0.0
    return {
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1_score": round(f1_score, 4),
        "accuracy": round(accuracy, 4),
        "detected_windows": int(prediction_array.sum()),
    }


def evaluate_all_detectors(
    features: pd.DataFrame,
    labels: pd.Series,
    train_mask: pd.Series,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fit detectors on clean reference windows and evaluate them."""
    train_features = features.loc[train_mask]
    clean_train_features = train_features.loc[labels.loc[train_mask] == 0]
    if clean_train_features.empty:
        raise ValueError("Clean training windows are required for unsupervised detectors.")

    detectors: dict[str, object] = {
        "statistical_thresholding": StatisticalThresholdDetector().fit(clean_train_features),
        "robust_zscore": RobustZScoreDetector().fit(clean_train_features),
        "pca_reconstruction": PCAReconstructionDetector().fit(clean_train_features),
    }

    predictions = pd.DataFrame(index=features.index)
    summaries: list[dict[str, float | str | int]] = []

    for detector_name, detector in detectors.items():
        predicted_labels = detector.predict(features)
        raw_scores = detector.score_samples(features)
        summary = _classification_summary(labels, predicted_labels)
        predictions[f"{detector_name}_score"] = np.round(raw_scores, 4)
        predictions[f"{detector_name}_prediction"] = predicted_labels
        summaries.append({"detector": detector_name, **summary})

    summary_df = pd.DataFrame(summaries).sort_values(["f1_score", "precision"], ascending=False)
    return predictions.reset_index(drop=True), summary_df.reset_index(drop=True)
