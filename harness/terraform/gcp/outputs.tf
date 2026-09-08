output "endpoints" {
  value = {
    for key, node in local.nodes : key => "http://${node.host}"
  }
}

output "bucket" {
  value = google_storage_bucket.data.name
}

output "meta_endpoint" {
  value = google_sql_database_instance.meta.first_ip_address
}

output "alb_dns_name" {
  value = google_compute_address.ingress.address
}

output "vpc_id" {
  value = local.network_id
}

output "bootstrap_secret_arn" {
  value = google_secret_manager_secret.bootstrap.id
}

output "bootstrap_secret" {
  value = google_secret_manager_secret.bootstrap.id
}

output "private_subnet_ids" {
  value = local.private_subnet_ids
}