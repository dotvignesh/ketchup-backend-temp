# Pipeline Runbook

This runbook is container-first: execute pipeline tasks in `docker compose` using `uv`.

## Scope

- Data acquisition, preprocessing, validation, anomaly/bias checks
- Analytics materialization for planner-facing feature tables
- DVC reproducibility and Airflow orchestration

## Components

- DVC pipeline: `dvc.yaml`
- Stage scripts: `scripts/*.py`
- Airflow DAGs: `pipelines/airflow/dags/*.py`

## Start Services

```bash
cd ketchup-backend
cp .env.example .env
docker compose --profile pipeline up --build -d db pipeline
```

The `pipeline` service includes pinned dependencies from:

- `requirements.txt`
- `requirements-pipeline.txt`

## DVC Commands

Initialize workspace:

```bash
docker compose --profile pipeline exec pipeline ./scripts/setup_dvc.sh
```

Run stages:

```bash
docker compose --profile pipeline exec pipeline uv run --no-project dvc repro
```

Inspect graph:

```bash
docker compose --profile pipeline exec pipeline uv run --no-project dvc dag
```

## Airflow Commands

Migrate metadata DB:

```bash
docker compose --profile pipeline exec pipeline uv run --no-project airflow db migrate
```

List and trigger DAGs:

```bash
docker compose --profile pipeline exec pipeline uv run --no-project airflow dags list
docker compose --profile pipeline exec pipeline uv run --no-project airflow dags trigger daily_analytics_materialization
docker compose --profile pipeline exec pipeline uv run --no-project airflow dags trigger ketchup_comprehensive_pipeline
```

Optional variable:

```bash
docker compose --profile pipeline exec pipeline uv run --no-project airflow variables set run_extended_bias_analysis true
```

## Tests

```bash
docker compose --profile pipeline exec pipeline uv run --no-project pytest tests/test_pipeline_components.py -v
```

## Output Artifacts

Generated locally (ignored by git):

- `data/raw/*`
- `data/processed/*`
- `data/metrics/*`
- `data/reports/*`
- `data/statistics/*`

Airflow metadata:

- stored in Docker volume `ketchup-backend_airflow_home`

Versioned metadata:

- `dvc.lock` (commit when stage definitions/dependencies change)

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| DVC import failures on host | host package drift | run DVC in `pipeline` container |
| Airflow import failures on host | host package drift | run Airflow in `pipeline` container |
| Pipeline scripts cannot import project modules | missing `PYTHONPATH` | use provided `pipeline` service (sets `PYTHONPATH=/app`) |

## Shutdown

```bash
docker compose --profile pipeline down -v --remove-orphans
```
