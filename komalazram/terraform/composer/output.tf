output "dag_gcs_prefix" {
  description = "The full GCS path to the Composer DAG bucket"
  value       = google_composer_environment.citibike_composer.config[0].dag_gcs_prefix
}

