resource "kubernetes_secret_v1" "bootstrap" {
  metadata {
    name      = "${var.project}-bootstrap"
    namespace = "default"
  }

  data = {
    token = google_secret_manager_secret_version.bootstrap.secret_data
  }

  type = "Opaque"
}

resource "kubernetes_service_v1" "node" {
  for_each = local.nodes

  metadata {
    name      = "${var.project}-${each.key}"
    namespace = "default"

    annotations = {
      "cloud.google.com/neg"            = "{\"ingress\": true}"
      "cloud.google.com/backend-config" = "{\"default\": \"${var.project}-${each.key}\"}"
    }
  }

  spec {
    selector = {
      "app.kubernetes.io/name" = var.project
      "app.kubernetes.io/node" = each.key
    }

    port {
      name        = "pico"
      port        = 4437
      target_port = 4437
      protocol    = "TCP"
    }

    port {
      name        = "admin"
      port        = 9090
      target_port = 9090
      protocol    = "TCP"
    }
  }
}

resource "kubernetes_manifest" "backend_config" {
  for_each = local.nodes

  manifest = {
    apiVersion = "cloud.google.com/v1"
    kind       = "BackendConfig"
    metadata = {
      name      = "${var.project}-${each.key}"
      namespace = "default"
    }
    spec = {
      healthCheck = {
        requestPath = "/ready"
        port        = 9090
        type        = "HTTP"
      }
    }
  }
}

resource "kubernetes_deployment_v1" "node" {
  for_each = local.nodes

  metadata {
    name      = "${var.project}-${each.key}"
    namespace = "default"
    labels = {
      "app.kubernetes.io/name" = var.project
      "app.kubernetes.io/node" = each.key
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        "app.kubernetes.io/name" = var.project
        "app.kubernetes.io/node" = each.key
      }
    }

    template {
      metadata {
        labels = {
          "app.kubernetes.io/name" = var.project
          "app.kubernetes.io/node" = each.key
        }
      }

      spec {
        service_account_name = kubernetes_service_account_v1.nodes.metadata[0].name

        container {
          name              = "pico"
          image             = var.image
          image_pull_policy = "IfNotPresent"

          command = ["serve"]
          args = [
            "--routing", local.routing,
            "--shutdown-drain-sec", "5",
          ]

          port {
            name           = "pico"
            container_port = 4437
          }

          port {
            name           = "admin"
            container_port = 9090
          }

          env {
            name  = "PICO_NODE_ID"
            value = tostring(each.value.id)
          }
          env {
            name  = "PICO_LISTEN"
            value = "0.0.0.0:4437"
          }
          env {
            name  = "PICO_ADMIN_LISTEN"
            value = "0.0.0.0:9090"
          }
          env {
            name  = "PICO_HTTP_ADDRESS"
            value = "http://${each.value.host}"
          }
          env {
            name  = "PICO_META_URL"
            value = local.meta_url
          }
          env {
            name  = "PICO_STORAGE"
            value = local.storage
          }
          env {
            name  = "PICO_AUTH"
            value = "required"
          }

          env {
            name = "PICO_AUTH_BOOTSTRAP_TOKEN"
            value_from {
              secret_key_ref {
                name = kubernetes_secret_v1.bootstrap.metadata[0].name
                key  = "token"
              }
            }
          }

          readiness_probe {
            http_get {
              path = "/ready"
              port = 9090
            }
            initial_delay_seconds = 10
            period_seconds        = 10
          }

          liveness_probe {
            http_get {
              path = "/health"
              port = 9090
            }
            initial_delay_seconds = 30
            period_seconds        = 20
          }
        }
      }
    }
  }

  depends_on = [google_sql_database.picomq, google_secret_manager_secret_version.bootstrap]
}
