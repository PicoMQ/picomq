resource "aws_lb" "kafka" {
  name               = "${var.project}-kafka"
  internal           = true
  load_balancer_type = "network"
  subnets            = local.private_subnet_ids

  enable_cross_zone_load_balancing = true

  tags = {
    Name = "${var.project}-kafka"
  }
}

resource "aws_lb_target_group" "kafka" {
  for_each = local.nodes

  name        = "${var.project}-kafka-${each.key}"
  port        = 9092
  protocol    = "TCP"
  vpc_id      = local.vpc_id
  target_type = "ip"

  health_check {
    enabled             = true
    protocol            = "HTTP"
    port                = "9090"
    path                = "/ready"
    matcher             = "200"
    interval            = 10
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }

  deregistration_delay = 30

  tags = {
    Name = "${var.project}-kafka-${each.key}"
  }
}

resource "aws_lb_listener" "kafka" {
  for_each = local.nodes

  load_balancer_arn = aws_lb.kafka.arn
  port              = each.value.kafka_port
  protocol          = "TCP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.kafka[each.key].arn
  }
}

resource "aws_route53_record" "kafka" {
  zone_id = aws_route53_zone.this.zone_id
  name    = local.kafka_host
  type    = "A"

  alias {
    name                   = aws_lb.kafka.dns_name
    zone_id                = aws_lb.kafka.zone_id
    evaluate_target_health = true
  }
}
