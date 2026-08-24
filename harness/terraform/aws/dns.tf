resource "aws_route53_zone" "this" {
  name = var.domain

  vpc {
    vpc_id = local.vpc_id
  }

  tags = {
    Name = var.project
  }
}

resource "aws_route53_record" "node" {
  for_each = local.nodes

  zone_id = aws_route53_zone.this.zone_id
  name    = each.value.host
  type    = "A"

  alias {
    name                   = aws_lb.this.dns_name
    zone_id                = aws_lb.this.zone_id
    evaluate_target_health = true
  }
}
