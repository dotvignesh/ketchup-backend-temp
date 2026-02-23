# Ketchup Backend

FastAPI backend for group planning, voting, invites, availability, and post-event feedback.

## Current Architecture

- `api/routes/*`: thin HTTP controllers
- `services/*`: business logic and data orchestration
- `agents/planning.py`: canonical LLM planner (OpenAI-compatible tool-calling)
- `database/*`: asyncpg connection and schema migration SQL
- `config/settings.py`: environment-based configuration

`api/main.py` wires app startup/shutdown:
- DB pool connect/disconnect
- planner HTTP client lifecycle
- background invite-expiry loop

Domain errors raised in services are mapped to HTTP responses through `ServiceError` handling in `api/main.py`.

## Runtime Requirements

- Python 3.12+
- PostgreSQL
- OpenAI-compatible chat completions endpoint for planner (default expects `/v1/chat/completions`)
- Optional Google Maps server key for tool grounding

Install:

```bash
cd ketchup-backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Run:

```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

Health:

```bash
curl http://localhost:8000/health
```

## Key Environment Variables

- `DATABASE_URL`
- `VLLM_BASE_URL`
- `VLLM_MODEL`
- `VLLM_API_KEY`
- `PLANNER_FALLBACK_ENABLED`
- `GOOGLE_MAPS_API_KEY`
- `BACKEND_INTERNAL_API_KEY`
- `FRONTEND_URL`
- `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`, `SMTP_FROM_EMAIL`

## Auth Boundary

Most application routes expect:
- `X-User-Id` (UUID)
- optional `X-Internal-Auth` when `BACKEND_INTERNAL_API_KEY` is configured

In local stack, frontend proxy injects these headers server-side.

## API Surface (Current)

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

## Planner Behavior (Current)

- Planner calls an OpenAI-compatible model via `VLLM_BASE_URL`.
- With `GOOGLE_MAPS_API_KEY`, planner uses tool grounding for places and directions.
- If structured planner output fails, backend can synthesize deterministic maps-grounded plans (`maps_fallback`) from gathered tool results.
- If planner fails and `PLANNER_FALLBACK_ENABLED=true`, generic fallback plans can be returned (`fallback`).

## Validation Commands

```bash
python3 -m compileall agents api services
```

If you use the local Docker stack, prefer running via `ketchup-local/docker-compose.yml`.
