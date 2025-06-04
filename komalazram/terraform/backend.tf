# backend.tf
terraform {
  backend "gcs" {
    bucket = "" # Your existing bucket name

  }
}