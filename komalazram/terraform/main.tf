module "storage" {
  source          = "./storage"
  bucket_name     = var.bucket_name
  project_id      = var.project_id
  bucket_location = var.bucket_location

}


module "composer" {
  source                   = "./composer"
  project_id               = var.project_id
  citibike_composer_name   = var.citibike_composer_name # This was missing!
  composer_region          = var.composer_region
  composer_service_account = var.composer_service_account
}