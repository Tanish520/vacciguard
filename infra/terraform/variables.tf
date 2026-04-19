variable "project_name" {
  description = "Project slug used in AWS names."
  type        = string
  default     = "vacciguard"
}

variable "environment_name" {
  description = "Deployment environment name."
  type        = string
  default     = "baseline"
}

variable "aws_region" {
  description = "AWS region for the baseline environment."
  type        = string
  default     = "ap-south-1"
}

variable "grafana_region" {
  description = "AWS region for Amazon Managed Grafana. Must be a supported AMG region."
  type        = string
  default     = "ap-southeast-1"
}

variable "eks_cluster_name_override" {
  description = "Optional explicit EKS cluster name."
  type        = string
  default     = ""
}

variable "s3_bucket_name_override" {
  description = "Optional explicit S3 bucket name."
  type        = string
  default     = ""
}

variable "redis_subnet_group_name" {
  description = "Name for the ElastiCache subnet group."
  type        = string
  default     = "vacciguard-baseline-redis-subnets"
}

variable "redis_vpc_id" {
  description = "VPC ID for the baseline Redis security group."
  type        = string
}

variable "redis_subnet_ids" {
  description = "Subnet IDs for the ElastiCache subnet group. Reuses the default subnets for the first baseline pass."
  type        = list(string)
}

variable "eks_subnet_ids" {
  description = "Subnet IDs for the baseline EKS cluster and node group. Reuses the same default subnets for the first baseline pass."
  type        = list(string)
}

variable "redis_node_type" {
  description = "ElastiCache node type for the baseline environment."
  type        = string
  default     = "cache.t4g.micro"
}

variable "eks_node_instance_types" {
  description = "Instance types for the baseline EKS managed node group."
  type        = list(string)
  default     = ["t3.medium"]
}

variable "eks_node_desired_size" {
  description = "Desired node count for the baseline EKS managed node group."
  type        = number
  default     = 2
}

variable "eks_node_min_size" {
  description = "Minimum node count for the baseline EKS managed node group."
  type        = number
  default     = 1
}

variable "eks_node_max_size" {
  description = "Maximum node count for the baseline EKS managed node group."
  type        = number
  default     = 2
}

variable "observability_namespace" {
  description = "Namespace that hosts the AWS-managed metrics collector."
  type        = string
  default     = "observability"
}

variable "amp_workspace_name" {
  description = "Optional explicit Amazon Managed Prometheus workspace alias."
  type        = string
  default     = ""
}

variable "grafana_workspace_name" {
  description = "Optional explicit Amazon Managed Grafana workspace name."
  type        = string
  default     = ""
}

variable "grafana_admin_email" {
  description = "Administrative email for Amazon Managed Grafana workspace access."
  type        = string
  default     = "ops@vacciguard.local"
}

variable "cloudwatch_log_retention_days" {
  description = "Retention in days for the application log groups exported from Terraform."
  type        = number
  default     = 14
}

variable "observability_mode" {
  description = "Observability mode exposed for docs and outputs."
  type        = string
  default     = "managed"
}
