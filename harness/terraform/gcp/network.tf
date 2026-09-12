resource "google_compute_network" "this" {
  count = local.create_network ? 1 : 0

  name                    = var.project
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "private" {
  count = local.create_network ? 2 : 0

  name          = "${var.project}-private-${count.index + 1}"
  ip_cidr_range = "10.42.${count.index + 10}.0/24"
  region        = var.region
  network       = google_compute_network.this[0].id

  private_ip_google_access = true
}

resource "google_compute_router" "this" {
  count = local.create_network ? 1 : 0

  name    = "${var.project}-router"
  region  = var.region
  network = google_compute_network.this[0].id

  bgp {
    asn = 64514
  }
}

resource "google_compute_router_nat" "this" {
  count = local.create_network ? 1 : 0

  name                               = "${var.project}-nat"
  router                             = google_compute_router.this[0].name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"

  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

data "google_compute_subnetwork" "existing_private" {
  count = local.create_network ? 0 : length(var.private_subnet_ids)

  self_link = var.private_subnet_ids[count.index]
}