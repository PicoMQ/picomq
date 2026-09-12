output "table_bucket_arn" {
  value = aws_s3tables_table_bucket.this.arn
}

output "application_name" {
  value = aws_kinesisanalyticsv2_application.this.name
}

output "log_group" {
  value = aws_cloudwatch_log_group.flink.name
}
