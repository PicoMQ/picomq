output "endpoints" {
  value = {
    for key, node in local.nodes : key => "http://${node.host}"
  }
}

output "bucket" {
  value = aws_s3_bucket.data.bucket
}

output "meta_endpoint" {
  value = aws_db_instance.meta.address
}

output "alb_dns_name" {
  value = aws_lb.this.dns_name
}

output "kafka_bootstrap" {
  value = "${local.kafka_host}:9092"
}

output "kafka_brokers" {
  value = {
    for key, node in local.nodes : key => node.kafka_advertise
  }
}

output "kafka_nlb_dns_name" {
  value = aws_lb.kafka.dns_name
}

output "vpc_id" {
  value = local.vpc_id
}

output "bootstrap_secret_arn" {
  value = aws_secretsmanager_secret.bootstrap.arn
}

output "private_subnet_ids" {
  value = local.private_subnet_ids
}
