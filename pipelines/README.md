# Data Pipeline

Pipeline code is Postgres-first and feeds planner quality.

## What It Does

- Extracts raw availability/feedback data from Postgres.
- Runs preprocessing and quality checks.
- Materializes planner-facing analytics tables:
  - `analytics.plan_outcome_fact`
  - `analytics.venue_performance_prior`
  - `analytics.group_feature_snapshot`

## Components

- `airflow/dags/daily_etl_dag.py`: daily analytics materialization.
- `airflow/dags/comprehensive_etl_dag.py`: materialization + validation + optional bias checks.
- `scripts/*.py`: DVC stage scripts.
- `dvc.yaml`: reproducible stage graph.

## Dependencies

Core backend deps are in `requirements.txt`.
Optional pipeline stack:

```bash
pip install -r requirements-pipeline.txt
```

## DVC Flow

```bash
dvc repro
dvc dag
```

Outputs:
- `data/raw/*`
- `data/processed/*`
- `data/metrics/*`
- `data/reports/*`
- `data/statistics/*`
- `dvc.lock`

The comprehensive DAG also writes `data/reports/pipeline_report.json` with per-task
durations and ranked bottlenecks.

Commit policy:
- Commit `dvc.yaml` and `dvc.lock` if stages/deps changed.
- Do not commit generated `data/*` outputs.

## Airflow Flow

```bash
export AIRFLOW_HOME="$(pwd)/airflow_home"
airflow db init
airflow dags list
airflow dags trigger daily_analytics_materialization
airflow dags trigger ketchup_comprehensive_pipeline
```

Optional heavy bias checks:

```bash
airflow variables set run_extended_bias_analysis true
```
