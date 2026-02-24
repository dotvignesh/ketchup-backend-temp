# Ketchup Data Pipeline Submission Notes

## Scope and Architecture

Pipeline and runtime share one source of truth: Postgres.

- Product data lives in core tables (`groups`, `plans`, `votes`, `events`, `feedback`, `availability_blocks`).
- Pipeline materializes planner-facing analytics tables in `analytics.*`.
- Planner reads those analytics features during generate/refine.

Primary code paths:

- DAGs:
  - `pipelines/airflow/dags/daily_etl_dag.py`
  - `pipelines/airflow/dags/comprehensive_etl_dag.py`
- DVC graph:
  - `dvc.yaml`
- Stage scripts:
  - `scripts/acquire_data.py`
  - `scripts/acquire_user_feedback.py`
  - `scripts/preprocess_data.py`
  - `scripts/validate_data.py`
  - `scripts/detect_anomalies.py`
  - `scripts/detect_bias.py`
  - `scripts/materialize_analytics.py`
  - `scripts/generate_statistics.py`
- Reusable modules:
  - `pipelines/preprocessing.py`
  - `pipelines/validation.py`
  - `pipelines/bias_detection.py`
  - `pipelines/monitoring.py`

## Rubric Mapping

1. Data acquisition
- Implemented in `scripts/acquire_data.py` and `scripts/acquire_user_feedback.py`.
- Pulls from live Postgres using `DATABASE_URL`.

2. Preprocessing
- Implemented in `scripts/preprocess_data.py` and `pipelines/preprocessing.py`.
- Handles cleaning, missing values, outliers, and feature engineering.

3. Test modules
- Implemented in `tests/test_pipeline_components.py`.
- Covers preprocessing, validation, monitoring, and bias slicing utilities.

4. Orchestration (Airflow)
- Daily materialization DAG: `daily_analytics_materialization`.
- Comprehensive DAG: `ketchup_comprehensive_pipeline`.
- Task dependencies are explicit and deterministic.

5. Versioning (DVC)
- Stage graph is defined in `dvc.yaml`.
- Setup helper: `scripts/setup_dvc.sh`.

6. Tracking and logging
- Structured logging helpers in `pipelines/monitoring.py`.
- Comprehensive DAG writes `data/reports/pipeline_report.json`.
- Performance section includes per-task runtime and top bottlenecks.

7. Schema/statistics generation
- Schema/quality checks in `scripts/validate_data.py`.
- Statistics generation in `scripts/generate_statistics.py`.

8. Anomaly detection and alerts
- Anomaly detection in `scripts/detect_anomalies.py`.
- Alerting primitives are implemented in `pipelines/monitoring.py` (`AnomalyAlert`).

9. Bias detection and mitigation
- Bias slicing and mitigation report generation in:
  - `scripts/detect_bias.py`
  - `pipelines/bias_detection.py`
- Slicing dimensions:
  - `availability_category` via `DataSlicer.slice_by_demographic`
  - optional multi-feature/grouped slicing via `slice_by_multiple_features` and `create_demographic_strata`
- Metrics tracked per slice:
  - selection rate (statistical parity)
  - true positive rate (TPR)
  - false positive rate (FPR)
  - disparate impact ratio utility for group-to-group comparison
- Mitigation techniques implemented/documented:
  - recommendation generation in `generate_mitigation_report` (active in `scripts/detect_bias.py`)
  - `BiasMitigationStrategy.resample_underrepresented`
  - `BiasMitigationStrategy.stratified_sampling`
- Trade-off documentation is captured in generated bias report outputs (`data/reports/bias_report.json`) and can be extended with model-level evaluation metrics.

10. Flow optimization
- Comprehensive DAG report includes:
  - per-task durations
  - ranked bottlenecks

11. Reproducibility
- Pipeline can be reproduced from repo + env config + Postgres source.
- DVC graph encodes inputs/outputs/metrics for deterministic stage execution.

12. Error handling
- Stage scripts use fail-fast behavior with explicit non-zero exits on failure.
- DAG tasks preserve status and runtime profiles, including failed runs.

## Generated Outputs

Typical generated artifacts:

- `data/raw/*.csv`
- `data/processed/*.csv`
- `data/metrics/*.json`
- `data/reports/*.json`
- `data/statistics/*.json`
- `dvc.lock`

Repository policy:

- Commit pipeline code/config and `dvc.lock` when stage graph changes.
- Do not commit generated `data/*` outputs.
