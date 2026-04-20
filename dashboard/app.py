"""Build a static HTML dashboard from generated monitoring results."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESULTS_ROOT = PROJECT_ROOT / "results"
OUTPUT_PATH = PROJECT_ROOT / "dashboard" / "index.html"


def _metric_card(label: str, value: str) -> str:
    return f"""
    <div class="card">
      <div class="card-label">{label}</div>
      <div class="card-value">{value}</div>
    </div>
    """


def _line_chart_svg(series: list[tuple[str, list[float], str]], width: int = 980, height: int = 320) -> str:
    left = 55
    top = 20
    chart_width = width - 90
    chart_height = height - 60
    all_values = [value for _, values, _ in series for value in values]
    if not all_values:
        all_values = [0.0]
    min_value = min(all_values)
    max_value = max(all_values)
    if max_value == min_value:
        max_value = min_value + 1.0

    elements = [
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + chart_height}" stroke="#52606d" stroke-width="2" />',
        f'<line x1="{left}" y1="{top + chart_height}" x2="{left + chart_width}" y2="{top + chart_height}" stroke="#52606d" stroke-width="2" />',
    ]

    for tick in range(5):
        y = top + chart_height * tick / 4
        elements.append(
            f'<line x1="{left}" y1="{y}" x2="{left + chart_width}" y2="{y}" stroke="#d9e2ec" stroke-width="1" />'
        )

    for label, values, color in series:
        points = []
        for index, value in enumerate(values):
            x = left + chart_width * index / max(1, len(values) - 1)
            normalized = (value - min_value) / (max_value - min_value)
            y = top + chart_height - normalized * chart_height
            points.append(f"{x:.2f},{y:.2f}")
            elements.append(f'<circle cx="{x:.2f}" cy="{y:.2f}" r="3.5" fill="{color}" />')
        elements.append(f'<polyline fill="none" stroke="{color}" stroke-width="3" points="{" ".join(points)}" />')

    legend_y = height - 20
    legend_items = []
    for index, (label, _, color) in enumerate(series):
        x = 70 + index * 210
        legend_items.append(f'<rect x="{x}" y="{legend_y}" width="14" height="14" fill="{color}" />')
        legend_items.append(f'<text x="{x + 22}" y="{legend_y + 12}" font-size="14" fill="#1f2933">{label}</text>')

    return f'<svg width="{width}" height="{height}">{"".join(elements)}{"".join(legend_items)}</svg>'


def build_dashboard() -> Path:
    batch_results = RESULTS_ROOT / "batch" / "quality_metrics.csv"
    detector_results = RESULTS_ROOT / "batch" / "detector_summary.csv"
    stream_results = RESULTS_ROOT / "stream" / "stream_quality_metrics.csv"

    if not batch_results.exists():
        OUTPUT_PATH.write_text(
            "<html><body><h1>Results not found</h1><p>Run the demo scripts before opening the dashboard.</p></body></html>",
            encoding="utf-8",
        )
        return OUTPUT_PATH

    batch_df = pd.read_csv(batch_results)
    detector_df = pd.read_csv(detector_results)
    stream_df = pd.read_csv(stream_results) if stream_results.exists() else pd.DataFrame()

    cards = "".join(
        [
            _metric_card("Batch windows", str(len(batch_df))),
            _metric_card("Injected anomalous batches", str(int(batch_df["is_anomalous"].sum()))),
            _metric_card("Average missing rate", f"{batch_df['missing_rate'].mean():.3f}"),
            _metric_card("Best detector", detector_df.iloc[0]["detector"]),
        ]
    )

    batch_chart = _line_chart_svg(
        [
            ("Missing rate", batch_df["missing_rate"].tolist(), "#2a9d8f"),
            ("Duplicate rate", batch_df["duplicate_rate"].tolist(), "#f4a261"),
            ("Distribution drift", batch_df["distribution_drift_score"].tolist(), "#457b9d"),
            ("Delayed record rate", batch_df["delayed_record_rate"].tolist(), "#e76f51"),
        ]
    )

    detector_chart = _line_chart_svg(
        [
            ("Precision", detector_df["precision"].tolist(), "#2a9d8f"),
            ("Recall", detector_df["recall"].tolist(), "#457b9d"),
            ("F1 score", detector_df["f1_score"].tolist(), "#e76f51"),
        ],
        width=760,
        height=280,
    )

    stream_section = ""
    if not stream_df.empty:
        stream_chart = _line_chart_svg(
            [
                ("Triggered rules", stream_df["rule_trigger_count"].tolist(), "#2a9d8f"),
                ("Thresholding", stream_df["statistical_thresholding_prediction"].tolist(), "#f4a261"),
                ("Robust z-score", stream_df["robust_zscore_prediction"].tolist(), "#457b9d"),
                ("PCA reconstruction", stream_df["pca_reconstruction_prediction"].tolist(), "#e76f51"),
            ]
        )
        stream_section = f"""
        <section>
          <h2>Streaming Simulation</h2>
          <div class="panel">{stream_chart}</div>
          <div class="panel table-panel">{stream_df[['chunk_name', 'anomaly_types', 'triggered_rules']].head(15).to_html(index=False)}</div>
        </section>
        """

    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <title>Data Quality Monitoring Dashboard</title>
      <style>
        body {{
          margin: 0;
          font-family: "Times New Roman", serif;
          color: #1f2933;
          background: #f4f6f8;
        }}
        header {{
          background: linear-gradient(120deg, #1d3557, #457b9d);
          color: white;
          padding: 28px 40px;
        }}
        main {{
          padding: 24px 34px 40px;
        }}
        .grid {{
          display: grid;
          grid-template-columns: repeat(4, minmax(180px, 1fr));
          gap: 18px;
          margin-bottom: 26px;
        }}
        .card, .panel {{
          background: white;
          border-radius: 14px;
          box-shadow: 0 6px 22px rgba(15, 23, 42, 0.08);
          padding: 18px 20px;
        }}
        .card-label {{
          font-size: 0.95rem;
          color: #52606d;
          margin-bottom: 10px;
        }}
        .card-value {{
          font-size: 1.8rem;
          font-weight: bold;
        }}
        section {{
          margin-bottom: 28px;
        }}
        h2 {{
          margin-bottom: 14px;
        }}
        .two-col {{
          display: grid;
          grid-template-columns: 1.3fr 1fr;
          gap: 20px;
        }}
        table {{
          width: 100%;
          border-collapse: collapse;
          font-size: 0.95rem;
        }}
        th, td {{
          border-bottom: 1px solid #e5e7eb;
          padding: 8px 10px;
          text-align: left;
        }}
        th {{
          background: #f8fafc;
        }}
      </style>
    </head>
    <body>
      <header>
        <h1>Data Quality Monitoring and Anomaly Detection</h1>
        <p>Semester 1 local prototype dashboard for the M.Tech major project.</p>
      </header>
      <main>
        <div class="grid">{cards}</div>
        <section>
          <h2>Batch Quality Timeline</h2>
          <div class="panel">{batch_chart}</div>
        </section>
        <section class="two-col">
          <div class="panel">
            <h2>Detector Comparison</h2>
            {detector_chart}
            {detector_df.to_html(index=False)}
          </div>
          <div class="panel table-panel">
            <h2>Flagged Batch Windows</h2>
            {batch_df.loc[batch_df['is_anomalous'] == 1, ['asset_name', 'anomaly_types', 'triggered_rules', 'statistical_thresholding_prediction', 'robust_zscore_prediction', 'pca_reconstruction_prediction']].to_html(index=False)}
          </div>
        </section>
        {stream_section}
      </main>
    </body>
    </html>
    """
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    return OUTPUT_PATH


if __name__ == "__main__":
    path = build_dashboard()
    print(f"Dashboard written to {path}")
