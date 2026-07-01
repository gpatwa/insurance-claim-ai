variable "kubeconfig" {
  type        = string
  default     = "~/.kube/config"
  description = "Path to kubeconfig for the target cluster (EKS/GKE/AKS/local)."
}

variable "release_name" {
  type    = string
  default = "claimpipe"
}

variable "namespace" {
  type    = string
  default = "claimpipe"
}

variable "image_repository" {
  type    = string
  default = "ghcr.io/gpatwa/claimpipe"
}

variable "image_tag" {
  type    = string
  default = "latest"
}

variable "config" {
  type        = map(string)
  description = "Adapter endpoints (Temporal/Postgres/S3/Kafka) — the per-cloud config."
  default     = {}
}
