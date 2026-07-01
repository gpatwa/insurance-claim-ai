# Deploy claimpipe to any Kubernetes cluster (EKS / GKE / AKS / local). The cluster and the
# backing services (Temporal, Postgres, Redpanda, object store) are provisioned separately or
# via managed offerings; this deploys the app chart. Portability = only var.config changes.

terraform {
  required_version = ">= 1.5"
  required_providers {
    kubernetes = { source = "hashicorp/kubernetes", version = "~> 2.30" }
    helm       = { source = "hashicorp/helm", version = "~> 2.14" }
  }
}

provider "kubernetes" {
  config_path = var.kubeconfig
}

provider "helm" {
  kubernetes {
    config_path = var.kubeconfig
  }
}

resource "helm_release" "claimpipe" {
  name             = var.release_name
  namespace        = var.namespace
  create_namespace = true
  chart            = "${path.module}/../../charts/claimpipe"

  set {
    name  = "image.repository"
    value = var.image_repository
  }
  set {
    name  = "image.tag"
    value = var.image_tag
  }

  # per-cloud adapter endpoints — the only thing that differs between targets
  dynamic "set" {
    for_each = var.config
    content {
      name  = "config.${set.key}"
      value = set.value
    }
  }
}
