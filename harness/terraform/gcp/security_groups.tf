resource "google_compute_firewall" "alb_to_nodes" {
  name    = "${var.project}-alb-to-nodes"
  network = local.network_id

  direction = "INGRESS"

  allow {
    protocol = "tcp"
    ports    = ["4437", "9090"]
  }

  source_ranges = ["10.42.0.0/16"]

  target_tags = ["${var.project}-node"]
}

resource "google_compute_firewall" "internal_admin" {
  name    = "${var.project}-internal-admin"
  network = local.network_id

  direction = "INGRESS"

  allow {
    protocol = "tcp"
    ports    = ["9090"]
  }

  source_ranges = ["10.42.0.0/16"]

  target_tags = ["${var.project}-node"]
}

