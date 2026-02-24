# Data Pipeline Assignment Mapping

This document maps the implemented pipeline to the `data_pipeline___MLOPS-1-2.pdf` rubric.

## Summary

The pipeline is Postgres-first and integrated with product runtime:

- Source: live product tables in Postgres.
- Processing: DVC stage graph + Airflow DAG orchestration.
- Output: analytics features in `analytics.*` tables, consumed by planner generate/refine flows.

## Architecture Coverage

Primary implementation paths:

- DVC graph: `dvc.yaml`
- Stage scripts: `scripts/acquire_data.py`, `scripts/acquire_user_feedback.py`, `scripts/preprocess_data.py`, `scripts/validate_data.py`, `scripts/detect_anomalies.py`, `scripts/detect_bias.py`, `scripts/materialize_analytics.py`, `scripts/generate_statistics.py`
- Reusable modules: `pipelines/preprocessing.py`, `pipelines/validation.py`, `pipelines/bias_detection.py`, `pipelines/monitoring.py`
- Airflow DAGs: `pipelines/airflow/dags/daily_etl_dag.py`, `pipelines/airflow/dags/comprehensive_etl_dag.py`

## Rubric Matrix

| Rubric Item | Implementation | Verification |
|---|---|---|
| Data acquisition | `acquire_data.py`, `acquire_user_feedback.py` | `dvc repro` stage success + generated raw CSVs |
| Preprocessing | `preprocess_data.py`, `pipelines/preprocessing.py` | processed outputs + unit tests |
| Test modules | `tests/test_pipeline_components.py` | `pytest tests/test_pipeline_components.py -v` |
| Orchestration | Airflow DAGs (`daily_analytics_materialization`, `ketchup_comprehensive_pipeline`) | DAG trigger + run state |
| Versioning | `dvc.yaml`, `dvc.lock` | `dvc dag`, lock updates |
| Tracking/logging | `pipelines/monitoring.py`, report artifacts | `data/reports/pipeline_report.json` |
| Schema/statistics | `validate_data.py`, `generate_statistics.py` | validation pass + statistics JSON |
| Anomaly detection | `detect_anomalies.py` | anomaly report outputs |
| Bias detection/mitigation | `detect_bias.py`, `pipelines/bias_detection.py` | bias report outputs + tests |
| Flow optimization | Comprehensive DAG profiling/bottleneck section | `pipeline_report.json` performance block |
| Reproducibility | DVC graph + deterministic scripts | repeated `dvc repro` no-op when unchanged |
| Error handling | fail-fast scripts + DAG status tracking | non-zero exits and logged failures |

## Execution Checklist

1. Start pipeline stack.

```bash
cd ketchup-backend
cp .env.example .env
docker compose --profile pipeline up --build -d db pipeline
```

2. Initialize DVC workspace.

```bash
docker compose --profile pipeline exec pipeline ./scripts/setup_dvc.sh
```

3. Reproduce the DVC pipeline and inspect graph.

```bash
docker compose --profile pipeline exec pipeline uv run --no-project dvc repro
docker compose --profile pipeline exec pipeline uv run --no-project dvc dag
```

4. Run pipeline unit tests.

```bash
docker compose --profile pipeline exec pipeline uv run --no-project pytest tests/test_pipeline_components.py -v
```

5. Trigger Airflow DAGs.

```bash
docker compose --profile pipeline exec pipeline uv run --no-project airflow db migrate
docker compose --profile pipeline exec pipeline uv run --no-project airflow dags trigger daily_analytics_materialization
docker compose --profile pipeline exec pipeline uv run --no-project airflow dags trigger ketchup_comprehensive_pipeline
```

## Generated Artifacts

Local generated artifacts:

- `data/raw/*.csv`
- `data/processed/*.csv`
- `data/metrics/*.json`
- `data/reports/*.json`
- `data/statistics/*.json`
- `dvc.lock`

Repository policy:

- Commit pipeline code/config updates.
- Commit `dvc.lock` when stage graph/dependencies change.
- Do not commit generated `data/*` outputs.

## Compatibility Notes

Pinned in `requirements-pipeline.txt`:

- `pathspec==0.11.2`
- `connexion<3`
- `pendulum<3`
- `Flask-Session==0.4.0`

These pins are required for stable `dvc` + `apache-airflow==2.7.2` behavior.
