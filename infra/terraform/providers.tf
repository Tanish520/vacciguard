provider "aws" {
  region = var.aws_region

  default_tags {
    tags = local.common_tags
  }
}

provider "aws" {
  alias  = "grafana"
  region = var.grafana_region

  default_tags {
    tags = local.common_tags
  }
}
