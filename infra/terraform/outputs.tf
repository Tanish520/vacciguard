output "eks_cluster_name" {
  value = local.eks_cluster_name
}

output "eks_cluster_endpoint" {
  value = aws_eks_cluster.baseline.endpoint
}

output "s3_bucket_name" {
  value = aws_s3_bucket.pipeline_data.bucket
}

output "redis_endpoint" {
  value = aws_elasticache_cluster.redis.cache_nodes[0].address
}

output "ebs_csi_role_arn" {
  value = aws_iam_role.ebs_csi.arn
}

output "pipeline_irsa_role_arn" {
  value = aws_iam_role.pipeline_irsa.arn
}

output "observability_mode" {
  value = var.observability_mode
}

output "grafana_region" {
  value = var.grafana_region
}

output "amp_workspace_arn" {
  value = aws_prometheus_workspace.observability.arn
}

output "amp_workspace_endpoint" {
  value = aws_prometheus_workspace.observability.prometheus_endpoint
}

output "grafana_workspace_id" {
  value = aws_grafana_workspace.managed.id
}

output "grafana_workspace_endpoint" {
  value = aws_grafana_workspace.managed.endpoint
}

output "adot_collector_role_arn" {
  value = aws_iam_role.adot_collector.arn
}

output "stream_processor_log_group_name" {
  value = aws_cloudwatch_log_group.stream_processor.name
}

output "replay_producer_log_group_name" {
  value = aws_cloudwatch_log_group.replay_producer.name
}

output "evaluation_controller_log_group_name" {
  value = aws_cloudwatch_log_group.evaluation_controller.name
}
