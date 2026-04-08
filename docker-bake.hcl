variable "REGISTRY_IMAGE" {
  default = "ghcr.io/sparksbenjamin/vanguarr"
}

group "default" {
  targets = ["image"]
}

target "docker-metadata-action" {}

target "image" {
  inherits = ["docker-metadata-action"]
  context = "."
  dockerfile = "Dockerfile"
  tags = ["${REGISTRY_IMAGE}:latest"]
  platforms = [
    "linux/amd64",
    "linux/arm64",
  ]
  labels = {
    "org.opencontainers.image.title" = "Vanguarr"
    "org.opencontainers.image.description" = "AI-driven proactive media curation bridge for the Arr stack."
  }
}

target "ci" {
  inherits = ["image"]
  output = ["type=image"]
}
