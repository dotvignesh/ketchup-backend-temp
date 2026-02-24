# GCP Deployment (Backend + vLLM + Analytics Job)

Deployment is split into independent workloads:
- `ketchup-backend` (Cloud Run service)
- `ketchup-vllm` (Cloud Run service or GPU node exposing OpenAI-compatible `/v1`)
- `ketchup-analytics-materialization` (Cloud Run Job, scheduler-triggered)

## 1) Required APIs

```bash
gcloud services enable \
  run.googleapis.com \
  cloudscheduler.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  secretmanager.googleapis.com \
  sqladmin.googleapis.com \
  storage.googleapis.com
```

## 2) Secrets

Create runtime secrets:

```bash
printf "%s" "$DATABASE_URL"         | gcloud secrets create DATABASE_URL --data-file=-
printf "%s" "$GOOGLE_MAPS_API_KEY" | gcloud secrets create GOOGLE_MAPS_API_KEY --data-file=-
printf "%s" "$TAVILY_API_KEY"      | gcloud secrets create TAVILY_API_KEY --data-file=-
printf "%s" "$VLLM_API_KEY"        | gcloud secrets create VLLM_API_KEY --data-file=-
printf "%s" "$BACKEND_INTERNAL_API_KEY" | gcloud secrets create BACKEND_INTERNAL_API_KEY --data-file=-
printf "%s" "$HF_TOKEN"            | gcloud secrets create HF_TOKEN --data-file=-
```

## 3) Deploy vLLM

Serve an OpenAI-compatible endpoint.
If using vLLM tool-calling:
- `--enable-auto-tool-choice`
- `--tool-call-parser hermes` (or parser matching model/template)

Capture URL:

```bash
VLLM_URL="$(gcloud run services describe ketchup-vllm --region "$REGION" --format='value(status.url)')"
echo "$VLLM_URL"
```

## 4) Deploy Backend

Backend environment:
- `DATABASE_URL` (Cloud SQL Postgres)
- `VLLM_BASE_URL=${VLLM_URL}/v1`
- `VLLM_MODEL=<served-model-name>`
- `VLLM_API_KEY`
- `GOOGLE_MAPS_API_KEY`
- `TAVILY_API_KEY`
- `BACKEND_INTERNAL_API_KEY`

## 5) Deploy Analytics Job

Run `python scripts/materialize_analytics.py` inside a Cloud Run Job image.
Schedule via Cloud Scheduler (daily or desired cadence).

Terraform path:
- `terraform/analytics_job.tf`
- `terraform/variables.tf`
- `terraform/terraform.tfvars.example`

## 6) Verify

Backend:

```bash
curl "${BACKEND_URL}/health"
```

vLLM:

```bash
curl "${VLLM_URL}/health"
curl "${VLLM_URL}/v1/models"
```

Analytics status endpoint (internal key required):

```bash
curl -H "X-Internal-Auth: ${BACKEND_INTERNAL_API_KEY}" \
  "${BACKEND_URL}/api/internal/analytics/status"
```

Planner path check:
- Generate/refine plans for a real group.
- Confirm plans include `logistics.analytics` metadata.
- Confirm backend logs show tool usage and no schema parse failures.
