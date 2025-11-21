### Text-to-SQL RFT with Eval Protocol (End-to-End)

This repository demonstrates an end-to-end Natural Language → SQL workflow using Eval Protocol and Fireworks RFT, without the old reward-kit. It includes:
- Data generation scripts (schema-only → synthetic DB → SQL → NL → train/test)
- A Dockerized MCP server that exposes a read-only DuckDB database over HTTP
- An Eval Protocol evaluator that executes model-generated SQL via MCP and scores results
- Local smoke tests and Makefile helpers

#### Prerequisites
- Python 3.11+ (recommend `uv` or `venv`)
- `FIREWORKS_API_KEY` (Fireworks account)
- Google Cloud SDK (for Cloud Run MCP deployment) if you want remote server
- Optional: `OPENAI_API_KEY`, `ANTHROPIC_API_KEY` for benchmarking additional models

#### Quickstart
1) Create a Python environment in this folder and install:
```
pip install -r requirements.txt
```

2) Generate data (OpenFlights → prod → synthetic → queries → ground-truth → NL):
```
make all-data
```

3) Build and deploy MCP server to Cloud Run:
```
make mcp-deploy PROJECT_ID=your-gcp-project REGION=us-central1
```
Copy the service URL (without trailing `/mcp/`). Set `MCP_SERVER_URL` for the evaluator.

4) Test evaluator locally:
```
pytest -q
```

5) Launch RFT (from `evaluator/` with `.env` containing FIREWORKS_API_KEY and MCP_SERVER_URL):
```
cd evaluator
eval-protocol create rft --base-model accounts/fireworks/models/qwen2p5-7b
```

6) Benchmark base vs tuned:
```
python scripts/benchmark_models.py
```

See `scripts/` for individual steps and `mcp_server/` for Docker deployment details.
