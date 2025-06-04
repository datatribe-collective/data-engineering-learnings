variable "project_id" {
  type        = string
  description = "GCP project ID where the bucket will be created"
}

variable "bucket_name" {
  type        = string
  description = "Name of the GCS bucket"
}

variable "bucket_location" {
  type        = string
  description = "GCS bucket location (region or multi-region)"
  default     = "EU"
}
