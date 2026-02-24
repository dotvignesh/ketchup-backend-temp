variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "analytics_job_image" {
  description = "Container image for analytics materialization job"
  type        = string
}

variable "analytics_job_service_account_email" {
  description = "Service account email used by analytics Cloud Run Job and Scheduler trigger"
  type        = string
}

variable "database_url_secret_name" {
  description = "Secret Manager secret name containing DATABASE_URL"
  type        = string
}

variable "analytics_job_schedule" {
  description = "Cron schedule for analytics materialization"
  type        = string
  default     = "0 3 * * *"
}
