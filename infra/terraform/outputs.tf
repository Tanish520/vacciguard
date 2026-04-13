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
