locals {
  name_prefix      = "${var.project_name}-${var.environment_name}"
  eks_cluster_name = var.eks_cluster_name_override != "" ? var.eks_cluster_name_override : "${local.name_prefix}-eks"
  s3_bucket_name   = var.s3_bucket_name_override != "" ? var.s3_bucket_name_override : "${local.name_prefix}-data"

  common_tags = {
    Project     = var.project_name
    Environment = var.environment_name
    ManagedBy   = "terraform"
    Deployment  = "baseline"
  }
}
