resource "google_service_account" "nodes" {
  account_id   = "${var.project}-nodes"
  display_name = "${var.project} PicoMQ nodes"
}

resource "google_storage_bucket_iam_member" "nodes_storage" {
  bucket = google_storage_bucket.data.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.nodes.email}"
}

resource "google_secret_manager_secret_iam_member" "nodes_bootstrap" {
  secret_id = google_secret_manager_secret.bootstrap.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.nodes.email}"
}

resource "kubernetes_service_account_v1" "nodes" {
  metadata {
    name      = "${var.project}-nodes"
    namespace = "default"

    annotations = {
      "iam.gke.io/gcp-service-account" = google_service_account.nodes.email
    }
  }

  depends_on = [google_container_cluster.this]
}

resource "google_service_account_iam_member" "workload_identity" {
  service_account_id = google_service_account.nodes.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project}.svc.id.goog[default/${kubernetes_service_account_v1.nodes.metadata[0].name}]"
}

