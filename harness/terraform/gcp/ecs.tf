resource "google_container_cluster" "this" {
  name     = var.project
  location = var.region

  network    = local.network_id
  subnetwork = local.private_subnet_ids[0]

  remove_default_node_pool = true
  initial_node_count       = 1

  workload_identity_config {
    workload_pool = "${var.project}.svc.id.goog"
  }

  release_channel {
    channel = "REGULAR"
  }

  deletion_protection = !var.force_destroy
}

resource "google_container_node_pool" "this" {
  name     = "${var.project}-nodes"
  location = var.region
  cluster  = google_container_cluster.this.name

  node_count = 1

  node_config {
    machine_type = "e2-standard-2"

    service_account = google_service_account.nodes.email

    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]

    workload_metadata_config {
      mode = "GKE_METADATA"
    }

    labels = {
      app = var.project
    }

    tags = ["${var.project}-node"]
  }
}