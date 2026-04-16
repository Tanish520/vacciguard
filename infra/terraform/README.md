# Terraform AWS Baseline Foundation

This directory holds the Terraform baseline AWS foundation for VacciGuard.
It can be initialized, planned, and applied against the live AWS account to
provision the baseline EKS cluster, S3 bucket, Redis stack, IAM roles, and
supporting network resources.

It also provisions the AWS-managed observability foundation used by the EKS
evaluation runs:

- Amazon Managed Service for Prometheus for metrics storage and queries
- Amazon Managed Grafana for dashboards
- CloudWatch log groups and the Amazon CloudWatch Observability add-on

## Scope

- provider and version configuration
- naming and tag conventions
- baseline S3 bucket scaffold
- baseline ElastiCache Redis scaffold
- environment variables and outputs for later workload wiring
- the live apply flow for the baseline foundation
- AWS-managed observability resources and outputs

## Managed Observability Notes

Amazon Managed Grafana requires IAM Identity Center to be enabled in the AWS
account. If the account does not have Identity Center configured yet, enable
it before running `terraform apply`.

Amazon Managed Grafana is provisioned in `ap-southeast-1` by default because
`ap-south-1` does not support AMG. The EKS cluster, AMP workspace, and CloudWatch
resources stay in `ap-south-1`; only Grafana moves to the supported region.

The observability outputs include:

- AMP workspace ARN and query endpoint
- Grafana workspace ID and URL
- Grafana workspace region
- CloudWatch log group names for the stream processor, replay producer, and
  evaluation controller

The managed Grafana dashboard uses the `pipeline_target` label to plot
baseline and optimized series in the same panels.

## Next Handoff

After `terraform apply`, use the outputs to hand off into cluster access and
workload setup:

```bash
aws eks update-kubeconfig --region ap-south-1 --name "$(terraform -chdir=infra/terraform output -raw eks_cluster_name)"
kubectl get nodes
```

To open the managed Grafana workspace, switch the AWS console region to
`ap-southeast-1` and use the `grafana_workspace_endpoint` output or the
workspace name shown by Terraform. The dashboard still reads from the Mumbai
AMP workspace, but Grafana itself lives in the supported Singapore region.

## Validation

```bash
terraform -chdir=infra/terraform init
terraform -chdir=infra/terraform fmt -check
terraform -chdir=infra/terraform validate
```

## Real Apply Flow

```bash
terraform -chdir=infra/terraform init
terraform -chdir=infra/terraform plan -out=tfplan
terraform -chdir=infra/terraform apply tfplan
terraform -chdir=infra/terraform output
```

## Local Terraform Artifacts

Terraform creates working files in this directory during plan/apply runs:

- `.terraform/`
- `tfplan`
- `terraform.tfstate`
- `terraform.tfstate.backup`

These are local operator artifacts, not source files. Do not commit them casually;
clean them up or leave them ignored as part of local workflow.
