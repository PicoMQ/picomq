data "aws_vpc" "selected" {
  id = local.vpc_id
}

resource "aws_security_group" "alb" {
  name_prefix = "${var.project}-alb-"
  description = "Internal ALB for PicoMQ protocol traffic"
  vpc_id      = local.vpc_id

  ingress {
    description = "HTTP from VPC"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.selected.cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project}-alb"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "task" {
  name_prefix = "${var.project}-task-"
  description = "PicoMQ Fargate tasks"
  vpc_id      = local.vpc_id

  ingress {
    description     = "Protocol from ALB"
    from_port       = 4437
    to_port         = 4437
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }

  ingress {
    description     = "Admin health from ALB"
    from_port       = 9090
    to_port         = 9090
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }

  ingress {
    description = "Admin from VPC"
    from_port   = 9090
    to_port     = 9090
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.selected.cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project}-task"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "rds" {
  name_prefix = "${var.project}-rds-"
  description = "PicoMQ metadata Postgres"
  vpc_id      = local.vpc_id

  ingress {
    description     = "Postgres from tasks"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.task.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project}-rds"
  }

  lifecycle {
    create_before_destroy = true
  }
}
