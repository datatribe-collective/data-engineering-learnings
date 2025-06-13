resource "google_composer_environment" "citibike_composer" {
  name   = var.citibike_composer_name
  region = var.composer_region
  config {
    software_config {
      image_version = "composer-3-airflow-2"
    }

    node_config {
      service_account = var.composer_service_account
    }
  }
}