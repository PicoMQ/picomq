resource "aws_security_group" "flink" {
  name_prefix = "${var.project}-flink-"
  description = "Managed Flink ENIs"
  vpc_id      = local.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project}-flink"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_cloudwatch_log_group" "flink" {
  name              = "/aws/kinesis-analytics/${var.project}"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_stream" "flink" {
  name           = "application"
  log_group_name = aws_cloudwatch_log_group.flink.name
}

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["kinesisanalytics.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "flink" {
  name               = "${var.project}-flink"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

data "aws_iam_policy_document" "flink" {
  statement {
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${aws_s3_bucket.code.arn}/*"]
  }

  statement {
    actions = ["s3tables:*"]
    resources = [
      aws_s3tables_table_bucket.this.arn,
      "${aws_s3tables_table_bucket.this.arn}/*",
    ]
  }

  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [local.token_secret]
  }

  statement {
    actions = [
      "logs:DescribeLogGroups",
      "logs:DescribeLogStreams",
      "logs:PutLogEvents",
    ]
    resources = ["*"]
  }

  statement {
    actions = [
      "ec2:DescribeVpcs",
      "ec2:DescribeSubnets",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeDhcpOptions",
      "ec2:DescribeNetworkInterfaces",
      "ec2:CreateNetworkInterface",
      "ec2:CreateNetworkInterfacePermission",
      "ec2:DeleteNetworkInterface",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "flink" {
  name   = "flink"
  role   = aws_iam_role.flink.id
  policy = data.aws_iam_policy_document.flink.json
}

resource "aws_kinesisanalyticsv2_application" "this" {
  name                   = var.project
  runtime_environment    = "FLINK-2_3"
  service_execution_role = aws_iam_role.flink.arn
  start_application      = true

  application_configuration {
    application_code_configuration {
      code_content_type = "ZIPFILE"
      code_content {
        s3_content_location {
          bucket_arn     = aws_s3_bucket.code.arn
          file_key       = aws_s3_object.jar.key
          object_version = aws_s3_object.jar.version_id
        }
      }
    }

    application_snapshot_configuration {
      snapshots_enabled = true
    }

    environment_properties {
      property_group {
        property_group_id = "picomq"
        property_map = {
          PICO_HTTP             = local.pico_http
          PICO_PREFIX           = var.prefix
          KAFKA_BOOTSTRAP       = local.kafka_bootstrap
          PICO_TOKEN_SECRET_ARN = local.token_secret
          TABLE_BUCKET_ARN      = aws_s3tables_table_bucket.this.arn
          NAMESPACE             = var.namespace
          REGION                = var.region
        }
      }
    }

    flink_application_configuration {
      checkpoint_configuration {
        configuration_type = "DEFAULT"
      }

      monitoring_configuration {
        configuration_type = "CUSTOM"
        log_level          = "INFO"
        metrics_level      = "APPLICATION"
      }

      parallelism_configuration {
        configuration_type   = "CUSTOM"
        parallelism          = 1
        parallelism_per_kpu  = 1
        auto_scaling_enabled = false
      }
    }

    vpc_configuration {
      security_group_ids = [aws_security_group.flink.id]
      subnet_ids         = local.subnet_ids
    }
  }

  cloudwatch_logging_options {
    log_stream_arn = aws_cloudwatch_log_stream.flink.arn
  }

  depends_on = [
    aws_iam_role_policy.flink,
    aws_s3tables_table.conversations,
    aws_s3tables_table.agent_events,
  ]
}
