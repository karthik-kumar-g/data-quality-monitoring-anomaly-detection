PYTHON ?= python3
PORT ?= 8000

.PHONY: demo dashboard serve clean

demo:
	$(PYTHON) scripts/generate_demo_data.py
	$(PYTHON) scripts/inject_anomalies.py
	$(PYTHON) scripts/run_batch_pipeline.py
	$(PYTHON) scripts/run_streaming_pipeline.py
	$(PYTHON) dashboard/app.py

dashboard:
	$(PYTHON) dashboard/app.py

serve:
	$(PYTHON) -m http.server $(PORT) -d dashboard

clean:
	rm -rf data/base_batches data/batch_inputs data/stream_inputs
	rm -f data/*.csv dashboard/index.html
	rm -rf results/batch results/stream results/logs results/figures
