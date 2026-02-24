# Enable required GCP APIs
resource "google_project_service" "enabled_apis" {
  for_each = toset([
    "run.googleapis.com",               # Cloud Run (backend + jobs)
    "cloudscheduler.googleapis.com",    # Scheduler for materialization jobs
    "secretmanager.googleapis.com",     # Secret Manager
    "artifactregistry.googleapis.com",  # Artifact Registry
    "storage.googleapis.com",           # Cloud Storage (optional DVC artifacts)
    "cloudbuild.googleapis.com",        # Cloud Build
    "sqladmin.googleapis.com"           # Cloud SQL (Postgres)
  ])

  project = var.project_id
  service = each.key

  disable_on_destroy = false
}
