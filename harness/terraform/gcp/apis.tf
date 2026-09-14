resource "google_project_service" "container" {
  service = "container.googleapis.com"

  disable_on_destroy = false
}

resource "google_project_service" "sqladmin" {
  service = "sqladmin.googleapis.com"

  disable_on_destroy = false
}

resource "google_project_service" "storage" {
  service = "storage.googleapis.com"

  disable_on_destroy = false
}

resource "google_project_service" "secretmanager" {
  service = "secretmanager.googleapis.com"

  disable_on_destroy = false
}

resource "google_project_service" "servicenetworking" {
  service = "servicenetworking.googleapis.com"

  disable_on_destroy = false
}

resource "google_project_service" "compute" {
  service = "compute.googleapis.com"

  disable_on_destroy = false
}