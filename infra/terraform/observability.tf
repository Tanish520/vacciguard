resource "aws_prometheus_workspace" "observability" {
  alias = local.amp_workspace_alias

  tags = local.common_tags
}

data "aws_iam_policy_document" "grafana_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["grafana.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "grafana_service" {
  name               = local.grafana_service_role_name
  assume_role_policy = data.aws_iam_policy_document.grafana_assume_role.json
}

data "aws_iam_policy_document" "grafana_access" {
  statement {
    actions = [
      "aps:ListWorkspaces",
      "aps:DescribeWorkspace",
      "aps:QueryMetrics",
      "aps:GetSeries",
      "aps:GetLabels",
      "aps:GetMetricMetadata",
      "aps:DescribeAlertManager",
    ]
    resources = ["*"]
  }

  statement {
    actions = [
      "cloudwatch:ListMetrics",
      "cloudwatch:GetMetricData",
      "cloudwatch:GetMetricStatistics",
      "cloudwatch:DescribeAlarms",
      "logs:DescribeLogGroups",
      "logs:DescribeLogStreams",
      "logs:GetLogEvents",
      "logs:FilterLogEvents",
      "logs:StartQuery",
      "logs:GetQueryResults",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "grafana_access" {
  name   = "${local.name_prefix}-grafana-access"
  role   = aws_iam_role.grafana_service.id
  policy = data.aws_iam_policy_document.grafana_access.json
}

resource "aws_grafana_workspace" "managed" {
  provider                 = aws.grafana
  name                     = local.grafana_workspace_name
  account_access_type      = "CURRENT_ACCOUNT"
  authentication_providers = ["AWS_SSO"]
  permission_type          = "SERVICE_MANAGED"
  role_arn                 = aws_iam_role.grafana_service.arn
  data_sources             = ["CLOUDWATCH", "PROMETHEUS"]
  description              = "VacciGuard AWS-managed dashboard workspace"

  lifecycle {
    create_before_destroy = true
  }

  tags = local.common_tags
}

resource "aws_eks_addon" "cloudwatch_observability" {
  cluster_name = aws_eks_cluster.baseline.name
  addon_name   = "amazon-cloudwatch-observability"

  depends_on = [
    aws_eks_node_group.baseline,
  ]
}

resource "aws_cloudwatch_log_group" "stream_processor" {
  name              = "/aws/vacciguard/${local.name_prefix}/stream-processor"
  retention_in_days = var.cloudwatch_log_retention_days
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "replay_producer" {
  name              = "/aws/vacciguard/${local.name_prefix}/replay-producer"
  retention_in_days = var.cloudwatch_log_retention_days
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "evaluation_controller" {
  name              = "/aws/vacciguard/${local.name_prefix}/evaluation-controller"
  retention_in_days = var.cloudwatch_log_retention_days
  tags              = local.common_tags
}

data "aws_iam_policy_document" "adot_collector_assume_role" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.eks.arn]
    }

    condition {
      # IRSA subject: system:serviceaccount:observability:adot-collector
      test     = "StringEquals"
      variable = "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:sub"
      values   = ["system:serviceaccount:${local.observability_namespace}:adot-collector"]
    }

    condition {
      test     = "StringEquals"
      variable = "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "adot_collector" {
  name               = local.adot_collector_role_name
  assume_role_policy = data.aws_iam_policy_document.adot_collector_assume_role.json
}

data "aws_iam_policy_document" "adot_collector_remote_write" {
  statement {
    actions   = ["aps:RemoteWrite"]
    resources = [aws_prometheus_workspace.observability.arn]
  }
}

resource "aws_iam_policy" "adot_collector_remote_write" {
  name   = "${local.name_prefix}-adot-collector-remote-write"
  policy = data.aws_iam_policy_document.adot_collector_remote_write.json
}

resource "aws_iam_role_policy_attachment" "adot_collector_remote_write" {
  role       = aws_iam_role.adot_collector.name
  policy_arn = aws_iam_policy.adot_collector_remote_write.arn
}
