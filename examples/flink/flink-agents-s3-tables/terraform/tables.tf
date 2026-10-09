resource "random_id" "suffix" {
  byte_length = 4
}

resource "aws_s3tables_table_bucket" "this" {
  name = "${var.project}-${random_id.suffix.hex}"
}

resource "aws_s3tables_namespace" "agents" {
  namespace        = var.namespace
  table_bucket_arn = aws_s3tables_table_bucket.this.arn
}

resource "aws_s3tables_table" "conversations" {
  name             = "conversations"
  namespace        = aws_s3tables_namespace.agents.namespace
  table_bucket_arn = aws_s3tables_table_bucket.this.arn
  format           = "ICEBERG"

  metadata {
    iceberg {
      schema {
        field {
          name     = "stream"
          type     = "string"
          required = true
        }
        field {
          name     = "seq"
          type     = "long"
          required = true
        }
        field {
          name = "role"
          type = "string"
        }
        field {
          name = "content"
          type = "string"
        }
        field {
          name = "ts"
          type = "timestamptz"
        }
      }
    }
  }
}

resource "aws_s3tables_table" "agent_events" {
  name             = "agent_events"
  namespace        = aws_s3tables_namespace.agents.namespace
  table_bucket_arn = aws_s3tables_table_bucket.this.arn
  format           = "ICEBERG"

  metadata {
    iceberg {
      schema {
        field {
          name     = "stream"
          type     = "string"
          required = true
        }
        field {
          name     = "seq"
          type     = "long"
          required = true
        }
        field {
          name = "type"
          type = "string"
        }
        field {
          name = "step_index"
          type = "int"
        }
        field {
          name = "finish_reason"
          type = "string"
        }
        field {
          name = "total_tokens"
          type = "long"
        }
        field {
          name = "tools"
          type = "string"
        }
        field {
          name = "ts"
          type = "timestamptz"
        }
      }
    }
  }
}

resource "aws_s3_bucket" "code" {
  bucket        = "${var.project}-code-${random_id.suffix.hex}"
  force_destroy = var.force_destroy
}

resource "aws_s3_bucket_public_access_block" "code" {
  bucket                  = aws_s3_bucket.code.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "code" {
  bucket = aws_s3_bucket.code.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_object" "jar" {
  bucket      = aws_s3_bucket.code.id
  key         = "flink-agents-s3-tables.jar"
  source      = var.jar_path
  source_hash = filemd5(var.jar_path)

  depends_on = [aws_s3_bucket_versioning.code]
}
