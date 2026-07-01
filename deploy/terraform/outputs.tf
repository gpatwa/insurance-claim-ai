output "release" {
  value = helm_release.claimpipe.name
}

output "namespace" {
  value = helm_release.claimpipe.namespace
}
