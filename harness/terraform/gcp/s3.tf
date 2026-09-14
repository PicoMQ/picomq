resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "google_storage_bucket" "data" {
  name          = "${var.project}-${random_id.bucket_suffix.hex}"
  location      = var.region
  force_destroy = var.force_destroy

  uniform_bucket_level_access = true

  public_access_prevention = "enforced"

  versioning {
    enabled = true
  }
}