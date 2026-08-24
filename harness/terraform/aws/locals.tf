locals {
  create_network = var.vpc_id == null

  create_s3_endpoint = local.create_network || var.create_s3_endpoint

  nodes = {
    for i in range(1, var.node_count + 1) : tostring(i) => {
      id   = i
      host = var.node_count == 1 ? var.domain : "pico-${i}.${var.domain}"
    }
  }

  routing = var.node_count == 1 ? "local" : "redirect"

  vpc_id = local.create_network ? aws_vpc.this[0].id : var.vpc_id

  private_subnet_ids = local.create_network ? aws_subnet.private[*].id : var.private_subnet_ids

  meta_url = "postgres://picomq:${var.db_password}@${aws_db_instance.meta.address}:5432/picomq"

  storage = "-2@s3://${aws_s3_bucket.data.bucket}?region=${var.region}"
}
