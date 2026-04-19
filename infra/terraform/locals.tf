locals {
  name_prefix               = "${var.project_name}-${var.environment_name}"
  eks_cluster_name          = var.eks_cluster_name_override != "" ? var.eks_cluster_name_override : "${local.name_prefix}-eks"
  s3_bucket_name            = var.s3_bucket_name_override != "" ? var.s3_bucket_name_override : "${local.name_prefix}-data"
  observability_namespace   = var.observability_namespace
  amp_workspace_alias       = var.amp_workspace_name != "" ? var.amp_workspace_name : "${local.name_prefix}-amp"
  grafana_workspace_name    = var.grafana_workspace_name != "" ? var.grafana_workspace_name : "${local.name_prefix}-grafana"
  adot_collector_role_name  = "${local.name_prefix}-observability-adot-collector"
  grafana_service_role_name = "${local.name_prefix}-grafana-service-role"

  common_tags = {
    Project     = var.project_name
    Environment = var.environment_name
    ManagedBy   = "terraform"
    Deployment  = "baseline"
  }
}
