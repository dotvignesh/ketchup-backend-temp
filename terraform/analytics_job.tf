# Cloud Run Job that materializes analytics feature tables in Postgres.
resource "google_cloud_run_v2_job" "analytics_materialization" {
  name     = "ketchup-analytics-materialization-${var.environment}"
  location = var.region

  template {
    template {
      service_account = var.analytics_job_service_account_email

      containers {
        image = var.analytics_job_image
        args  = ["python", "scripts/materialize_analytics.py"]

        env {
          name = "DATABASE_URL"
          value_source {
            secret_key_ref {
              secret  = var.database_url_secret_name
              version = "latest"
            }
          }
        }
      }

      max_retries = 1
      timeout     = "1200s"
    }
  }

  depends_on = [google_project_service.enabled_apis]
}

# Cloud Scheduler trigger for recurring job execution.
resource "google_cloud_scheduler_job" "analytics_materialization_trigger" {
  name      = "ketchup-analytics-materialization-${var.environment}"
  region    = var.region
  schedule  = var.analytics_job_schedule
  time_zone = "Etc/UTC"

  http_target {
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.analytics_materialization.name}:run"
    http_method = "POST"

    oauth_token {
      service_account_email = var.analytics_job_service_account_email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }

  depends_on = [google_cloud_run_v2_job.analytics_materialization]
}
