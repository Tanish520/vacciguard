resource "aws_s3_bucket" "pipeline_data" {
  bucket = local.s3_bucket_name
}

resource "aws_s3_bucket_versioning" "pipeline_data" {
  bucket = aws_s3_bucket.pipeline_data.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_security_group" "redis" {
  name        = "${local.name_prefix}-redis"
  description = "Baseline Redis security group"
  vpc_id      = var.redis_vpc_id

  tags = {
    Name = "${local.name_prefix}-redis"
  }
}

data "aws_vpc" "redis" {
  id = var.redis_vpc_id
}

resource "aws_security_group_rule" "redis_ingress_vpc" {
  type              = "ingress"
  security_group_id = aws_security_group.redis.id
  from_port         = 6379
  to_port           = 6379
  protocol          = "tcp"
  cidr_blocks       = [data.aws_vpc.redis.cidr_block]
}

resource "aws_elasticache_subnet_group" "redis" {
  name       = var.redis_subnet_group_name
  subnet_ids = var.redis_subnet_ids
}

resource "aws_elasticache_cluster" "redis" {
  cluster_id           = "${local.name_prefix}-redis"
  engine               = "redis"
  node_type            = var.redis_node_type
  num_cache_nodes      = 1
  parameter_group_name = "default.redis7"
  port                 = 6379
  security_group_ids   = [aws_security_group.redis.id]
  subnet_group_name    = aws_elasticache_subnet_group.redis.name
}

resource "aws_eks_cluster" "baseline" {
  name     = local.eks_cluster_name
  role_arn = aws_iam_role.eks_cluster.arn

  vpc_config {
    subnet_ids = var.eks_subnet_ids
  }

  depends_on = [
    aws_iam_role_policy_attachment.eks_cluster_policy,
  ]
}

resource "aws_eks_node_group" "baseline" {
  cluster_name    = aws_eks_cluster.baseline.name
  node_group_name = "${local.name_prefix}-nodes"
  node_role_arn   = aws_iam_role.eks_node.arn
  subnet_ids      = var.eks_subnet_ids
  instance_types  = var.eks_node_instance_types

  scaling_config {
    desired_size = var.eks_node_desired_size
    min_size     = var.eks_node_min_size
    max_size     = var.eks_node_max_size
  }

  depends_on = [
    aws_iam_role_policy_attachment.eks_worker_node_policy,
    aws_iam_role_policy_attachment.eks_cni_policy,
    aws_iam_role_policy_attachment.eks_ecr_read_only,
  ]
}

resource "aws_eks_addon" "ebs_csi" {
  cluster_name             = aws_eks_cluster.baseline.name
  addon_name               = "aws-ebs-csi-driver"
  service_account_role_arn = aws_iam_role.ebs_csi.arn

  depends_on = [
    aws_eks_node_group.baseline,
    aws_iam_role_policy_attachment.ebs_csi_driver,
  ]
}
