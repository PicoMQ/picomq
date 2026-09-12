resource "google_dns_managed_zone" "this" {
  name        = replace(var.project, "_", "-")
  dns_name    = "${var.domain}."
  description = "Private PicoMQ service discovery zone"

  visibility = "private"

  private_visibility_config {
    networks {
      network_url = local.network_id
    }
  }
}

resource "google_dns_record_set" "node" {
  for_each = local.nodes

  managed_zone = google_dns_managed_zone.this.name
  name         = "${each.value.host}."
  type         = "A"
  ttl          = 30
  rrdatas      = [google_compute_address.ingress.address]
}
