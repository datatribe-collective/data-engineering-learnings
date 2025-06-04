variable "composer_region" {
  type        = string
  description = "Region for Composer environment"
  default     = "europe-west4" # Same as bucket_location by default
}
variable "project_id" {
  type        = string
  description = "GCP project ID where the bucket will be created"
}

variable "citibike_composer_name" {
  type        = string
  description = "name for Composer environment"
}
variable "composer_service_account" {
  description = "The email address of the service account used by Cloud Composer"
  type        = string
}

