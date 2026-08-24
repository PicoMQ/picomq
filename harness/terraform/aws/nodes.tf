resource "aws_lb_target_group" "node" {
  for_each = local.nodes

  name        = "${var.project}-${each.key}"
  port        = 4437
  protocol    = "HTTP"
  vpc_id      = local.vpc_id
  target_type = "ip"

  health_check {
    enabled             = true
    path                = "/ready"
    port                = "9090"
    protocol            = "HTTP"
    matcher             = "200"
    interval            = 10
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }

  deregistration_delay = 30

  tags = {
    Name = "${var.project}-${each.key}"
  }
}

resource "aws_lb_listener_rule" "node" {
  for_each = local.nodes

  listener_arn = aws_lb_listener.http.arn
  priority     = each.value.id

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.node[each.key].arn
  }

  condition {
    host_header {
      values = [each.value.host]
    }
  }
}

resource "aws_ecs_task_definition" "node" {
  for_each = local.nodes

  family                   = "${var.project}-${each.key}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = tostring(var.task_cpu)
  memory                   = tostring(var.task_memory)
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([
    {
      name      = "pico"
      image     = var.image
      essential = true
      command = [
        "serve",
        "--routing", local.routing,
        "--shutdown-drain-sec", "5",
      ]
      portMappings = [
        {
          containerPort = 4437
          hostPort      = 4437
          protocol      = "tcp"
        },
        {
          containerPort = 9090
          hostPort      = 9090
          protocol      = "tcp"
        },
      ]
      environment = [
        { name = "PICO_NODE_ID", value = tostring(each.value.id) },
        { name = "PICO_LISTEN", value = "0.0.0.0:4437" },
        { name = "PICO_ADMIN_LISTEN", value = "0.0.0.0:9090" },
        { name = "PICO_HTTP_ADDRESS", value = "http://${each.value.host}" },
        { name = "PICO_META_URL", value = local.meta_url },
        { name = "PICO_STORAGE", value = local.storage },
        { name = "PICO_AUTH", value = "required" },
        { name = "AWS_REGION", value = var.region },
      ]
      secrets = [
        {
          name      = "PICO_AUTH_BOOTSTRAP_TOKEN"
          valueFrom = aws_secretsmanager_secret.bootstrap.arn
        },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.pico.name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = each.key
        }
      }
    }
  ])

  tags = {
    Name = "${var.project}-${each.key}"
  }
}

resource "aws_ecs_service" "node" {
  for_each = local.nodes

  name            = "${var.project}-${each.key}"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.node[each.key].arn
  desired_count   = 1
  launch_type     = "FARGATE"

  platform_version = "LATEST"

  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  network_configuration {
    subnets          = local.private_subnet_ids
    security_groups  = [aws_security_group.task.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.node[each.key].arn
    container_name   = "pico"
    container_port   = 4437
  }

  depends_on = [
    aws_lb_listener_rule.node,
    aws_db_instance.meta,
    aws_secretsmanager_secret_version.bootstrap,
  ]

  tags = {
    Name = "${var.project}-${each.key}"
  }
}
