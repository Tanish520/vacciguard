# Terraform AWS Baseline Foundation

This directory holds the Terraform baseline AWS foundation for VacciGuard.
It can be initialized, planned, and applied against the live AWS account to
provision the baseline EKS cluster, S3 bucket, Redis stack, IAM roles, and
supporting network resources.

## Scope

- provider and version configuration
- naming and tag conventions
- baseline S3 bucket scaffold
- baseline ElastiCache Redis scaffold
- environment variables and outputs for later workload wiring
- the live apply flow for the baseline foundation

## Next Handoff

After `terraform apply`, use the outputs to hand off into cluster access and
workload setup:

```bash
aws eks update-kubeconfig --region ap-south-1 --name "$(terraform -chdir=infra/terraform output -raw eks_cluster_name)"
kubectl get nodes
```

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
