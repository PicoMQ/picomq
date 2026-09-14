locals {
  create_network = var.vpc_id == null

  nodes = {
    for i in range(1, var.node_count + 1) : tostring(i) => {
      id   = i
      host = var.node_count == 1 ? var.domain : "pico-${i}.${var.domain}"
    }
  }

  routing = var.node_count == 1 ? "local" : "redirect"

  network_id = local.create_network ? google_compute_network.this[0].id : var.vpc_id

  private_subnet_ids = local.create_network ? google_compute_subnetwork.private[*].id : [
    for subnet in data.google_compute_subnetwork.existing_private : subnet.id
  ]

  meta_url = "postgres://picomq:${var.db_password}@${google_sql_database_instance.meta.first_ip_address}:5432/picomq"

  storage = "-2@gcs://${google_storage_bucket.data.name}"
}