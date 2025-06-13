output "composer_bucket_name" {
  description = "Composer GCS bucket name only (no gs:// or folder)"
  value       = split("/", replace(module.composer.dag_gcs_prefix, "gs://", ""))[0]
}
