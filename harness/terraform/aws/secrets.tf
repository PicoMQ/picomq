resource "random_bytes" "bootstrap_secret" {
  count = var.bootstrap_token == null ? 1 : 0

  length = 32
}

locals {
  bootstrap_id_b64 = replace(replace(replace(base64encode("aws/root"), "+", "-"), "/", "_"), "=", "")

  bootstrap_secret_b64 = (
    length(random_bytes.bootstrap_secret) > 0
    ? replace(replace(replace(random_bytes.bootstrap_secret[0].base64, "+", "-"), "/", "_"), "=", "")
    : null
  )

  bootstrap_token_value = (
    var.bootstrap_token != null
    ? var.bootstrap_token
    : "${local.bootstrap_id_b64}.${local.bootstrap_secret_b64}"
  )
}

resource "aws_secretsmanager_secret" "bootstrap" {
  name_prefix             = "${var.project}-bootstrap-"
  recovery_window_in_days = var.force_destroy ? 0 : 7

  tags = {
    Name = "${var.project}-bootstrap"
  }
}

resource "aws_secretsmanager_secret_version" "bootstrap" {
  secret_id     = aws_secretsmanager_secret.bootstrap.id
  secret_string = local.bootstrap_token_value
}
