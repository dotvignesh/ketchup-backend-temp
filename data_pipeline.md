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
- Model-specific slicing scripts for Section 2.4 / 2.5 deliverables:
  - `scripts/run_model_bias_synthetic_eval.py`
  - `scripts/check_model_bias_slices.py`
  - `scripts/check_model_bias_fairlearn.py`
  - `pipelines/model_bias.md`
- Slicing dimensions:
  - `availability_category` via `DataSlicer.slice_by_demographic`
  - optional multi-feature/grouped slicing via `slice_by_multiple_features` and `create_demographic_strata`
  - model-eval slices such as `city_tier`, `budget_tier`, `distance_bucket`, and `car_ratio_bucket`
- Metrics tracked per slice:
  - selection rate (statistical parity)
  - true positive rate (TPR)
  - false positive rate (FPR)
  - disparate impact ratio utility for group-to-group comparison
  - model-eval metrics such as `budget_compliance`, `distance_compliance`, and `logistics_feasible`
- Mitigation techniques implemented/documented:
  - recommendation generation in `generate_mitigation_report` (active in `scripts/detect_bias.py`)
  - `BiasMitigationStrategy.resample_underrepresented`
  - `BiasMitigationStrategy.stratified_sampling`
  - model-side guardrails documented in `pipelines/model_bias.md`:
    - budget prefiltering
    - validate then repair
    - low-coverage fallback mode
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

## Planner Evaluation Addendum

For the planner/model portion of the assignment, the repo now includes a domain-valid synthetic tool-calling benchmark instead of relying on a generic function-calling dataset.

- Benchmark driver: `scripts/evaluate_tool_calling_bfcl.py`
- Benchmark data: `data/benchmarks/synthetic_group_outings_tool_calling.json`
- Output artifact: `data/reports/tool_calling_group_outings_synthetic_25.json`

Why this was added:

- Domain validity: benchmark prompts are synthetic but shaped like actual Ketchup outing-planning requests, including budgets, accessibility needs, dietary constraints, nightlife/event cases, and ambiguous cases where the model should abstain from calling a tool.
- Tool-calling evaluation: benchmark cases explicitly test whether the model chooses the right planner tool (`search_places`, `get_directions`, `web_search`) and whether it correctly decides not to call a tool when critical grounding information is missing.
- Experiment tracking: Weights & Biases logging is integrated into the benchmark runner so each example can be tracked with running metrics, final summary metrics, and per-example outputs for comparison across model/config versions.
- Argument scoring realism: tool arguments are not graded with brittle strict-equality matching; instead, the served model acts as an LLM judge to score semantic argument quality while tool/no-tool and tool-name checks remain exact.

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
