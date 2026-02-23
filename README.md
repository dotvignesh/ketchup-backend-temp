# Ketchup Backend

FastAPI backend for groups, planning rounds, voting, invites, availability, and feedback.

## Architecture

- `api/routes/*`: HTTP handlers.
- `services/*`: business logic and persistence orchestration.
- `agents/planning.py`: canonical planner orchestration.
- `database/*`: asyncpg connection and schema SQL.
- `config/settings.py`: environment-backed settings.

## Requirements

- Python 3.12+
- PostgreSQL 16+
- OpenAI-compatible chat completions endpoint (vLLM recommended)

## Setup

```bash
cd ketchup-backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

Health check:

```bash
curl http://localhost:8000/health
```

## Key Environment Variables

Core:
- `DATABASE_URL`
- `BACKEND_INTERNAL_API_KEY`
- `FRONTEND_URL`

Planner:
- `VLLM_BASE_URL` (OpenAI-compatible base URL, usually ending in `/v1`)
- `VLLM_MODEL` (model name sent in chat completion requests)
- `VLLM_API_KEY`
- `PLANNER_NOVELTY_TARGET_GENERATE` (default `0.7`)
- `PLANNER_NOVELTY_TARGET_REFINE` (default `0.35`)
- `PLANNER_FALLBACK_ENABLED`

Tooling:
- `GOOGLE_MAPS_API_KEY` (Places API New + Routes API)
- `TAVILY_API_KEY` (optional web-search fallback)

Email:
- `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`, `SMTP_FROM_EMAIL`

## Planner Runtime Contract

- Planner uses OpenAI-compatible chat completions via `VLLM_BASE_URL`.
- Request shape is vLLM/OpenAI-compatible (no llama.cpp-specific fields).
- Tool loop does not force `tool_choice="auto"`.
- For vLLM tool-calling, run server with:
  - `--enable-auto-tool-choice`
  - `--tool-call-parser hermes` (or a parser matching your model/template)

Behavior:
- With `GOOGLE_MAPS_API_KEY`, planner uses `search_places` and `get_directions`.
- With `TAVILY_API_KEY`, planner can use `web_search` when map results are insufficient.
- If model output is empty/unparseable, backend attempts deterministic grounded synthesis.
- If enabled, generic fallback can be used as last resort.

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

Refine request body (optional):
- `descriptors: string[]`
- `lead_note: string`

Feedback:
- `POST /api/groups/{group_id}/events/{event_id}/feedback`
- `GET /api/groups/{group_id}/events/{event_id}/feedback`

## Validation

```bash
python3 -m compileall agents api services config models
```
