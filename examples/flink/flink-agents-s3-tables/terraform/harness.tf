data "terraform_remote_state" "harness" {
  backend = "s3"
  config = {
    bucket = var.harness_state_bucket
    key    = var.harness_state_key
    region = var.region
  }
}

locals {
  harness         = data.terraform_remote_state.harness.outputs
  pico_http       = local.harness.endpoints["1"]
  kafka_bootstrap = local.harness.kafka_bootstrap
  vpc_id          = local.harness.vpc_id
  subnet_ids      = local.harness.private_subnet_ids
  token_secret    = local.harness.bootstrap_secret_arn
}
