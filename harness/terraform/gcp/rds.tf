resource "google_sql_database_instance" "meta" {
  name             = "${var.project}-meta"
  database_version = "POSTGRES_16"
  region           = var.region

  settings {
    tier                  = var.db_instance_class
    disk_type             = "PD_SSD"
    disk_size             = 20
    disk_autoresize       = true
    disk_autoresize_limit = 100

    backup_configuration {
      enabled                        = true
      point_in_time_recovery_enabled = true
    }

    ip_configuration {
      ipv4_enabled    = false
      private_network = local.network_id
    }

    availability_type = var.db_multi_az ? "REGIONAL" : "ZONAL"
  }

  deletion_protection = !var.force_destroy

  depends_on = [google_service_networking_connection.private_vpc_connection]
}

resource "google_compute_global_address" "private_service_access" {
  name          = "${var.project}-private-services"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = local.network_id
}

resource "google_service_networking_connection" "private_vpc_connection" {
  network                 = local.network_id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_service_access.name]

  depends_on = [google_project_service.servicenetworking]
}

resource "google_sql_database" "picomq" {
  name     = "picomq"
  instance = google_sql_database_instance.meta.name
}

resource "google_sql_user" "picomq" {
  name     = "picomq"
  instance = google_sql_database_instance.meta.name
  password = var.db_password
}