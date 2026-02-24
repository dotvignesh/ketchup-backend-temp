# Ketchup Backend

FastAPI backend for group coordination: plan generation/refinement, voting, event finalization, and feedback.

## Architecture

- `api/routes/*`: HTTP handlers.
- `services/*`: business rules and persistence orchestration.
- `agents/planning.py`: planner orchestration (OpenAI-compatible API + tools).
- `analytics/*`: Postgres-backed feature materialization consumed by planner/refine.
- `database/*`: asyncpg pool and SQL migrations.
- `pipelines/*`: optional Airflow/DVC orchestration over the same Postgres source of truth.

## Runtime Data Model

Product and analytics both use Postgres:
- Product tables: `users`, `groups`, `group_preferences`, `plan_rounds`, `plans`, `votes`, `events`, `feedback`, `availability_blocks`.
- Analytics tables: `analytics.pipeline_runs`, `analytics.plan_outcome_fact`, `analytics.venue_performance_prior`, `analytics.group_feature_snapshot`.

## Setup

```bash
cd ketchup-backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

Optional pipeline dependencies:

```bash
pip install -r requirements-pipeline.txt
```

## Database Migrations

Initial schema:
- `database/migrations/01_schema.sql`

Analytics schema:
- `database/migrations/02_analytics.sql`

Backend startup and analytics jobs run an idempotent bootstrap that creates `analytics.*` tables
if missing. `02_analytics.sql` remains the source-of-truth migration for managed environments.

## Planner Runtime Contract

- Planner targets `VLLM_BASE_URL` (OpenAI-compatible `/v1` API).
- No llama.cpp-specific request fields are sent.
- Tooling:
  - `search_places` + `get_directions` when `GOOGLE_MAPS_API_KEY` is set.
  - optional `web_search` when `TAVILY_API_KEY` is set and maps results are insufficient.
- Planner reads analytics snapshots/venue priors from Postgres to improve generate/refine quality.

For vLLM tool-calling:
- `--enable-auto-tool-choice`
- `--tool-call-parser hermes` (or parser matching your model/template)

## Analytics Materialization

Run one refresh:

```bash
python scripts/materialize_analytics.py
```

This updates:
- `analytics.plan_outcome_fact`
- `analytics.venue_performance_prior`
- `analytics.group_feature_snapshot`
- `analytics.pipeline_runs`

Artifacts written for local inspection:
- `data/metrics/materialization_metrics.json`
- `data/reports/analytics_status.json`

## DVC Pipeline (Optional)

```bash
dvc repro
dvc dag
```

Assignment mapping document:
- `data_pipeline.md`

Policy:
- Commit: `dvc.yaml`, `dvc.lock` when stages/deps change.
- Do not commit generated `data/*` outputs.

## Airflow DAGs (Optional)

- `daily_analytics_materialization`
- `ketchup_comprehensive_pipeline`

Example:

```bash
export AIRFLOW_HOME="$(pwd)/airflow_home"
airflow db init
airflow dags list
airflow dags trigger daily_analytics_materialization
```

## Key Environment Variables

Core:
- `DATABASE_URL`
- `BACKEND_INTERNAL_API_KEY`
- `FRONTEND_URL`

Planner:
- `VLLM_BASE_URL`
- `VLLM_MODEL`
- `VLLM_API_KEY`
- `PLANNER_NOVELTY_TARGET_GENERATE`
- `PLANNER_NOVELTY_TARGET_REFINE`
- `PLANNER_FALLBACK_ENABLED`

Tools:
- `GOOGLE_MAPS_API_KEY`
- `TAVILY_API_KEY`

## API Surface

Auth:
- `POST /api/auth/google-signin`

Users:
- `GET /api/users/me`
- `PUT /api/users/me/preferences`
- `GET /api/users/me/availability`
- `PUT /api/users/me/availability`

Groups:
- `POST /api/groups`
- `GET /api/groups`
- `GET /api/groups/{group_id}`
- `PUT /api/groups/{group_id}`
- `POST /api/groups/{group_id}/invite`
- `POST /api/groups/{group_id}/invite/accept`
- `POST /api/groups/{group_id}/invite/reject`
- `PUT /api/groups/{group_id}/preferences`
- `POST /api/groups/{group_id}/availability`

Plans:
- `POST /api/groups/{group_id}/generate-plans`
- `GET /api/groups/{group_id}/plans/{round_id}`
- `POST /api/groups/{group_id}/plans/{round_id}/vote`
- `GET /api/groups/{group_id}/plans/{round_id}/results`
- `POST /api/groups/{group_id}/plans/{round_id}/refine`
- `POST /api/groups/{group_id}/plans/{round_id}/finalize`

Feedback:
- `POST /api/groups/{group_id}/events/{event_id}/feedback`
- `GET /api/groups/{group_id}/events/{event_id}/feedback`

Internal analytics (requires `X-Internal-Auth`):
- `GET /api/internal/analytics/status`
- `POST /api/internal/analytics/rebuild`

## Testing and Validation

Static sanity:

```bash
python3 -m compileall agents analytics api services config models database utils pipelines scripts
```

Pipeline tests:

```bash
pip install -r requirements-pipeline.txt
pytest tests/test_pipeline_components.py -v
```

Analytics materialization check:

```bash
python scripts/materialize_analytics.py
```

Local-stack API smoke (from `ketchup-local`):

```bash
docker compose -f ../ketchup-local/docker-compose.yml exec -T backend python -c "import httpx; print(httpx.get('http://localhost:8000/health', timeout=5).status_code)"
```
