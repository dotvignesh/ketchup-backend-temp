# GCP Deployment (Backend + vLLM)

This project deploys as two services:
- `ketchup-backend` (FastAPI application)
- `ketchup-vllm` (OpenAI-compatible vLLM inference)

## 1) Required APIs

```bash
gcloud services enable \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  secretmanager.googleapis.com \
  sqladmin.googleapis.com
```

## 2) Secrets

Create secrets used by backend and/or vLLM:

```bash
printf "%s" "$GOOGLE_MAPS_API_KEY" | gcloud secrets create GOOGLE_MAPS_API_KEY --data-file=-
printf "%s" "$TAVILY_API_KEY"      | gcloud secrets create TAVILY_API_KEY --data-file=-
printf "%s" "$VLLM_API_KEY"        | gcloud secrets create VLLM_API_KEY --data-file=-
printf "%s" "$HF_TOKEN"            | gcloud secrets create HF_TOKEN --data-file=-
```

## 3) Deploy vLLM Service

Deploy vLLM as its own Cloud Run service with GPU and tool-calling enabled.

Key runtime args:
- `--enable-auto-tool-choice`
- `--tool-call-parser hermes` (or parser matching your model/template)

vLLM should expose OpenAI-compatible routes at `/v1/*`.

After deploy, capture URL:

```bash
VLLM_URL="$(gcloud run services describe ketchup-vllm --region "$REGION" --format='value(status.url)')"
echo "$VLLM_URL"
```

## 4) Deploy Backend Service

Set backend env so planner calls vLLM:
- `VLLM_BASE_URL=${VLLM_URL}/v1`
- `VLLM_MODEL=<served model name>`
- `VLLM_API_KEY` (if required by vLLM service)

Also set:
- `DATABASE_URL`
- `GOOGLE_MAPS_API_KEY`
- `TAVILY_API_KEY`
- `BACKEND_INTERNAL_API_KEY`
- SMTP env vars if invite emails are enabled

## 5) Verify

Backend health:

```bash
curl "${BACKEND_URL}/health"
```

vLLM health/models:

```bash
curl "${VLLM_URL}/health"
curl "${VLLM_URL}/v1/models"
```

Planner path check:
- Create or use an existing group.
- Call backend `POST /api/groups/{group_id}/generate-plans`.
- Confirm backend logs show planner tool activity and no tool-choice parser errors.

## Notes

- Keep backend and vLLM deployments independent for scaling and release isolation.
- Backend planner supports any OpenAI-compatible endpoint, but vLLM is the primary target.
- If tool-calling is disabled on vLLM, backend will fall back to deterministic synthesis paths.
