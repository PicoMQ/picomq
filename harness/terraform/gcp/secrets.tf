resource "random_bytes" "bootstrap_secret" {
  count = var.bootstrap_token == null ? 1 : 0

  length = 32
}

locals {
  bootstrap_id_b64 = replace(
    replace(
      replace(base64encode("gcp/root"), "+", "-"),
      "/",
      "_"
    ),
    "=",
    ""
  )

  bootstrap_secret_b64 = (
    length(random_bytes.bootstrap_secret) > 0
    ? replace(
      replace(
        replace(random_bytes.bootstrap_secret[0].base64, "+", "-"),
        "/",
        "_"
      ),
      "=",
      ""
    )
    : null
  )

  bootstrap_token_value = (
    var.bootstrap_token != null
    ? var.bootstrap_token
    : "${local.bootstrap_id_b64}.${local.bootstrap_secret_b64}"
  )
}

resource "google_secret_manager_secret" "bootstrap" {
  secret_id = "${var.project}-bootstrap"

  replication {
    auto {}
  }

  deletion_protection = !var.force_destroy
}

resource "google_secret_manager_secret_version" "bootstrap" {
  secret      = google_secret_manager_secret.bootstrap.id
  secret_data = local.bootstrap_token_value
}