resource "aws_cloudwatch_log_group" "pico" {
  name              = "/ecs/${var.project}"
  retention_in_days = 30

  tags = {
    Name = var.project
  }
}

resource "aws_ecs_cluster" "this" {
  name = var.project

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = {
    Name = var.project
  }
}
