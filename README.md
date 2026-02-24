# Ketchup Backend

FastAPI backend plus data pipeline for planning, voting, and analytics feature materialization.

## What This Repo Owns

- API and business logic (`api/`, `services/`, `agents/`)
- Postgres schema and analytics tables (`database/migrations/`)
- Data pipeline stages, DVC graph, and Airflow DAGs (`scripts/`, `dvc.yaml`, `pipelines/`)

## Recommended Runtime (Docker + uv)

Use this repository's Compose stack to avoid maintaining multiple local Python environments.

1) Configure environment (Note: Do not worry about having keys for running Data Pipeline):

```bash
cd ketchup-backend
cp .env.example .env
```

2) Start API + Postgres:

```bash
docker compose up --build db api
```

3) Start pipeline worker (separate terminal):

```bash
docker compose --profile pipeline up --build -d db pipeline
```

4) Run pipeline actions through the pipeline container:

```bash
docker compose --profile pipeline exec pipeline uv run --no-project dvc repro -f
docker compose --profile pipeline exec pipeline uv run --no-project pytest tests/test_pipeline_components.py -v
docker compose --profile pipeline exec pipeline uv run --no-project dvc dag
docker compose --profile pipeline exec pipeline uv run --no-project airflow db migrate
docker compose --profile pipeline exec pipeline uv run --no-project airflow dags trigger daily_analytics_materialization # If this fails, please run the above 2 commands again (i.e.: dvc dav and db migrate)
docker compose --profile pipeline exec pipeline uv run --no-project airflow dags trigger ketchup_comprehensive_pipeline
docker compose --profile pipeline exec pipeline uv run --no-project airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@local \
    --password admin
docker compose --profile pipeline exec pipeline uv run --no-project airflow scheduler

# Run the following command in another terminal
docker compose --profile pipeline run --rm -p 8081:8081 pipeline uv run --no-project airflow webserver --port 8081
```

Notes:

- API container uses Python 3.12 (`Dockerfile`).
- Pipeline container uses Python 3.11 for Airflow compatibility (`Dockerfile.pipeline`).
- Both images install dependencies with `uv`.
- Backend Compose DB is internal-only (no host port binding) to avoid clashes with `ketchup-local`.

## Environment Variables

Core:

- `DATABASE_URL`
- `DATABASE_URL_INTERNAL` (Compose internal DB URL; defaults to `postgresql://postgres:postgres@db:5432/appdb`)
- `FRONTEND_URL`
- `BACKEND_INTERNAL_API_KEY`

Planner endpoint:

- `VLLM_BASE_URL` (OpenAI-compatible `/v1` endpoint)
- `VLLM_BASE_URL_INTERNAL` (Compose internal LLM URL; defaults to `http://host.docker.internal:8080/v1`)
- `VLLM_MODEL`
- `VLLM_API_KEY`

Planner behavior:

- `PLANNER_NOVELTY_TARGET_GENERATE`
- `PLANNER_NOVELTY_TARGET_REFINE`
- `PLANNER_FALLBACK_ENABLED`

Tooling:

- `GOOGLE_MAPS_API_KEY` for Maps tools
- `TAVILY_API_KEY` for web search fallback

## Planner Runtime Contract

- Planner calls an OpenAI-compatible chat completions API.
- No llama.cpp-specific fields are sent.
- Tool-calling is used when server/model supports it.
- If tool output is invalid/empty, deterministic grounded fallback is used.

For vLLM auto tool-calling, run vLLM with:

- `--enable-auto-tool-choice`
- `--tool-call-parser <model-compatible-parser>`

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| `dvc` fails with `_DIR_MARK` import error | `pathspec` drift | run inside pipeline container (pinned deps) |
| Airflow import errors (`flask_session` / `connexion`) | package drift in host venv | run Airflow via pipeline container |
| Planner tool loop disabled by server | vLLM missing tool-call flags | add `--enable-auto-tool-choice --tool-call-parser ...` |

## Related Docs

- `data_pipeline.md`
- `gcp.md`
- `agents/README.md`
