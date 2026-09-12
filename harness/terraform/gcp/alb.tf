resource "google_compute_address" "ingress" {
  name         = "${var.project}-ingress"
  address_type = "INTERNAL"
  subnetwork   = local.private_subnet_ids[0]
  region       = var.region
}

resource "kubernetes_ingress_v1" "internal" {
  metadata {
    name      = "${var.project}-internal"
    namespace = "default"

    annotations = {
      "kubernetes.io/ingress.class"                   = "gce-internal"
      "kubernetes.io/ingress.allow-http"              = "true"
      "kubernetes.io/ingress.regional-static-ip-name" = google_compute_address.ingress.name
    }
  }

  spec {
    dynamic "rule" {
      for_each = local.nodes
      content {
        host = rule.value.host

        http {
          path {
            path      = "/"
            path_type = "Prefix"

            backend {
              service {
                name = kubernetes_service_v1.node[rule.key].metadata[0].name
                port { number = 4437 }
              }
            }
          }
        }
      }
    }
  }

  depends_on = [kubernetes_service_v1.node]
}
