# AWS Baseline Default VPC Apply Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the reviewed AWS baseline scaffold into a real baseline AWS deployment in account `347038623570` and region `ap-south-1`, using the default VPC for speed.

**Architecture:** Extend the current Terraform scaffold to discover and use the default VPC, provision baseline AWS resources including EKS, S3, and ElastiCache, then wire the Kubernetes baseline overlay with the resulting live values and apply it to the new cluster. Keep the deployment fixed-capacity and baseline-only, with no monitoring or optimization in this pass.

**Tech Stack:** AWS CLI, Terraform, Amazon EKS, Amazon S3, Amazon ElastiCache Redis, Kubernetes, `kubectl`, Kustomize, Python

---

### Task 1: Lock the live default-VPC AWS inputs into Terraform

**Files:**
- Modify: `infra/terraform/variables.tf`
- Modify: `infra/terraform/terraform.tfvars.example`
- Create: `infra/terraform/terraform.tfvars`

- [ ] **Step 1: Write the failing network discovery check**

Run:

```bash
aws ec2 describe-vpcs \
  --filters Name=isDefault,Values=true \
  --region ap-south-1 \
  --query 'Vpcs[].VpcId' \
  --output text
```

Expected: PASS and return `vpc-0edff0449c4cff998`.

Run:

```bash
aws ec2 describe-subnets \
  --filters Name=default-for-az,Values=true \
  --region ap-south-1 \
  --query 'Subnets[].SubnetId' \
  --output text
```

Expected: PASS and return the three default subnet IDs:

- `subnet-0d5bb5a7096b51927`
- `subnet-0096327117649f0d2`
- `subnet-09b6f616b606abd4d`

- [ ] **Step 2: Make the Terraform network variables required for apply**

Update `infra/terraform/variables.tf` so these variables no longer default to empty values:

```hcl
variable "redis_vpc_id" {
  description = "VPC ID for the baseline Redis security group."
  type        = string
}

variable "redis_subnet_ids" {
  description = "Subnet IDs for the ElastiCache subnet group."
  type        = list(string)
}
```

Add these new variables:

```hcl
variable "eks_subnet_ids" {
  description = "Subnet IDs for the baseline EKS cluster and node group."
  type        = list(string)
}
```

- [ ] **Step 3: Update the example tfvars file with the discovered network values**

Update `infra/terraform/terraform.tfvars.example` to include:

```hcl
project_name               = "vacciguard"
environment_name           = "baseline"
aws_region                 = "ap-south-1"
eks_cluster_name_override  = ""
s3_bucket_name_override    = ""
redis_subnet_group_name    = "vacciguard-baseline-redis-subnets"
redis_vpc_id               = "vpc-0edff0449c4cff998"
redis_subnet_ids           = [
  "subnet-0d5bb5a7096b51927",
  "subnet-0096327117649f0d2",
  "subnet-09b6f616b606abd4d",
]
eks_subnet_ids             = [
  "subnet-0d5bb5a7096b51927",
  "subnet-0096327117649f0d2",
  "subnet-09b6f616b606abd4d",
]
redis_node_type            = "cache.t4g.micro"
```

- [ ] **Step 4: Create the real local tfvars file for apply**

Create `infra/terraform/terraform.tfvars` with:

```hcl
project_name               = "vacciguard"
environment_name           = "baseline"
aws_region                 = "ap-south-1"
eks_cluster_name_override  = ""
s3_bucket_name_override    = ""
redis_subnet_group_name    = "vacciguard-baseline-redis-subnets"
redis_vpc_id               = "vpc-0edff0449c4cff998"
redis_subnet_ids           = [
  "subnet-0d5bb5a7096b51927",
  "subnet-0096327117649f0d2",
  "subnet-09b6f616b606abd4d",
]
eks_subnet_ids             = [
  "subnet-0d5bb5a7096b51927",
  "subnet-0096327117649f0d2",
  "subnet-09b6f616b606abd4d",
]
redis_node_type            = "cache.t4g.micro"
```

- [ ] **Step 5: Verify Terraform formatting**

Run:

```bash
terraform -chdir=infra/terraform fmt
terraform -chdir=infra/terraform fmt -check
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add infra/terraform/variables.tf infra/terraform/terraform.tfvars.example infra/terraform/terraform.tfvars
git commit -m "infra: add default vpc baseline inputs"
```

### Task 2: Add the EKS foundation to Terraform

**Files:**
- Modify: `infra/terraform/variables.tf`
- Modify: `infra/terraform/main.tf`
- Modify: `infra/terraform/outputs.tf`
- Create: `infra/terraform/iam.tf`

- [ ] **Step 1: Write the failing Terraform validate check**

Run:

```bash
terraform -chdir=infra/terraform init
terraform -chdir=infra/terraform validate
```

Expected: PASS for the current scaffold, but no EKS resources exist yet.

Run:

```bash
rg -n "aws_eks_cluster|aws_eks_node_group" infra/terraform
```

Expected: FAIL or return no matches.

- [ ] **Step 2: Add EKS capacity variables**

Append to `infra/terraform/variables.tf`:

```hcl
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
```

- [ ] **Step 3: Create the IAM roles for EKS**

Create `infra/terraform/iam.tf` with:

```hcl
data "aws_iam_policy_document" "eks_cluster_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "eks_cluster" {
  name               = "${local.name_prefix}-eks-cluster-role"
  assume_role_policy = data.aws_iam_policy_document.eks_cluster_assume_role.json
}

resource "aws_iam_role_policy_attachment" "eks_cluster_policy" {
  role       = aws_iam_role.eks_cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

data "aws_iam_policy_document" "eks_node_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "eks_node" {
  name               = "${local.name_prefix}-eks-node-role"
  assume_role_policy = data.aws_iam_policy_document.eks_node_assume_role.json
}

resource "aws_iam_role_policy_attachment" "eks_worker_node_policy" {
  role       = aws_iam_role.eks_node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
}

resource "aws_iam_role_policy_attachment" "eks_cni_policy" {
  role       = aws_iam_role.eks_node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
}

resource "aws_iam_role_policy_attachment" "eks_ecr_read_only" {
  role       = aws_iam_role.eks_node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}
```

- [ ] **Step 4: Add the EKS cluster and node group**

Append to `infra/terraform/main.tf`:

```hcl
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
```

- [ ] **Step 5: Add EKS outputs**

Update `infra/terraform/outputs.tf` to include:

```hcl
output "eks_cluster_endpoint" {
  value = aws_eks_cluster.baseline.endpoint
}
```

- [ ] **Step 6: Verify Terraform**

Run:

```bash
terraform -chdir=infra/terraform fmt
terraform -chdir=infra/terraform validate
```

Expected: PASS after `terraform init` has already been run.

- [ ] **Step 7: Commit**

```bash
git add infra/terraform/variables.tf infra/terraform/main.tf infra/terraform/outputs.tf infra/terraform/iam.tf
git commit -m "infra: add baseline eks foundation"
```

### Task 3: Apply the baseline AWS foundation

**Files:**
- Modify: `infra/terraform/README.md`

- [ ] **Step 1: Capture the active caller identity**

Run:

```bash
aws sts get-caller-identity
```

Expected: account `347038623570`.

- [ ] **Step 2: Initialize Terraform**

Run:

```bash
terraform -chdir=infra/terraform init
```

Expected: PASS with AWS provider initialized.

- [ ] **Step 3: Run Terraform plan**

Run:

```bash
terraform -chdir=infra/terraform plan -out=tfplan
```

Expected: PASS and show creation of S3, Redis, IAM, EKS cluster, and EKS node group resources.

- [ ] **Step 4: Apply the baseline foundation**

Run:

```bash
terraform -chdir=infra/terraform apply tfplan
```

Expected: PASS with created baseline AWS resources.

- [ ] **Step 5: Capture Terraform outputs**

Run:

```bash
terraform -chdir=infra/terraform output
```

Expected: PASS with at least cluster name, cluster endpoint, bucket name, and Redis endpoint.

- [ ] **Step 6: Update the Terraform README for real apply flow**

Append this section to `infra/terraform/README.md`:

```md
## Real Apply Flow

```bash
terraform -chdir=infra/terraform init
terraform -chdir=infra/terraform plan -out=tfplan
terraform -chdir=infra/terraform apply tfplan
terraform -chdir=infra/terraform output
```
```

- [ ] **Step 7: Commit**

```bash
git add infra/terraform/README.md
git commit -m "docs: add terraform apply flow"
```

### Task 4: Configure kubectl for the live EKS cluster

**Files:**
- Modify: `docs/aws-baseline-foundation.md`

- [ ] **Step 1: Update kubeconfig from Terraform output**

Run:

```bash
aws eks update-kubeconfig --region ap-south-1 --name "$(terraform -chdir=infra/terraform output -raw eks_cluster_name)"
```

Expected: PASS and merge the baseline cluster context into local kubeconfig.

- [ ] **Step 2: Verify cluster connectivity**

Run:

```bash
kubectl get nodes
```

Expected: PASS and show the EKS node group nodes in `Ready` state.

- [ ] **Step 3: Document the kubeconfig step**

Append this section to `docs/aws-baseline-foundation.md`:

```md
## EKS Access

```bash
aws eks update-kubeconfig --region ap-south-1 --name "$(terraform -chdir=infra/terraform output -raw eks_cluster_name)"
kubectl get nodes
```
```

- [ ] **Step 4: Commit**

```bash
git add docs/aws-baseline-foundation.md
git commit -m "docs: add eks access steps"
```

### Task 5: Inject live AWS values into the Kubernetes baseline overlay

**Files:**
- Modify: `infra/kubernetes/baseline/configmap-pipeline.yaml`
- Modify: `docs/aws-baseline-foundation.md`

- [ ] **Step 1: Write the failing placeholder check**

Run:

```bash
rg -n "REPLACE_ME_BUCKET|REPLACE_ME_REDIS_HOST" infra/kubernetes/baseline/configmap-pipeline.yaml
```

Expected: PASS and show placeholder values still present.

- [ ] **Step 2: Replace the placeholder values with live Terraform outputs**

Use these commands to capture outputs:

```bash
terraform -chdir=infra/terraform output -raw s3_bucket_name
terraform -chdir=infra/terraform output -raw redis_endpoint
```

Update `infra/kubernetes/baseline/configmap-pipeline.yaml` so it becomes:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: vacciguard-pipeline-config
data:
  PROCESSED_OUTPUT_PATH: s3a://REPLACE_WITH_REAL_BUCKET/processed
  INVALID_OUTPUT_PATH: s3a://REPLACE_WITH_REAL_BUCKET/invalid
  CHECKPOINT_ROOT: s3a://REPLACE_WITH_REAL_BUCKET/checkpoints
  BREACH_WINDOW_OUTPUT_PATH: s3a://REPLACE_WITH_REAL_BUCKET/breach_windows
  REDIS_HOST: REPLACE_WITH_REAL_REDIS_ENDPOINT
```
```

Replace the two `REPLACE_WITH_*` placeholders with the real Terraform output values from the live account.

- [ ] **Step 3: Verify placeholders are gone**

Run:

```bash
rg -n "REPLACE_ME_BUCKET|REPLACE_ME_REDIS_HOST" infra/kubernetes/baseline/configmap-pipeline.yaml
```

Expected: FAIL with no matches.

- [ ] **Step 4: Document the live-value injection step**

Append this section to `docs/aws-baseline-foundation.md`:

```md
## Live Value Injection

Use Terraform outputs to replace the baseline overlay placeholders:

```bash
terraform -chdir=infra/terraform output -raw s3_bucket_name
terraform -chdir=infra/terraform output -raw redis_endpoint
```
```

- [ ] **Step 5: Commit**

```bash
git add infra/kubernetes/baseline/configmap-pipeline.yaml docs/aws-baseline-foundation.md
git commit -m "infra: wire live baseline aws values"
```

### Task 6: Deploy the baseline manifests to EKS

**Files:**
- Modify: `docs/aws-baseline-foundation.md`

- [ ] **Step 1: Re-render the overlays with live values**

Run:

```bash
kubectl kustomize infra/kubernetes/base > /tmp/vacciguard-base.yaml
kubectl kustomize infra/kubernetes/baseline > /tmp/vacciguard-baseline.yaml
```

Expected: PASS.

- [ ] **Step 2: Apply the baseline namespace and workloads**

Run:

```bash
kubectl apply -k infra/kubernetes/baseline
```

Expected: PASS and create or update the baseline resources in the cluster.

- [ ] **Step 3: Verify the baseline resources exist**

Run:

```bash
kubectl get all -n vacciguard
```

Expected: PASS and show Kafka, stream processor, and replay producer resources in the namespace.

- [ ] **Step 4: Document the apply step**

Append this section to `docs/aws-baseline-foundation.md`:

```md
## Baseline Deployment Apply

```bash
kubectl apply -k infra/kubernetes/baseline
kubectl get all -n vacciguard
```
```

- [ ] **Step 5: Commit**

```bash
git add docs/aws-baseline-foundation.md
git commit -m "docs: add baseline apply steps"
```

### Task 7: Final verification pass

**Files:**
- Modify: any of the files above only if verification reveals a real defect

- [ ] **Step 1: Reconfirm AWS identity**

Run:

```bash
aws sts get-caller-identity
```

Expected: account `347038623570`.

- [ ] **Step 2: Verify Terraform still parses and formats**

Run:

```bash
terraform -chdir=infra/terraform fmt -check
terraform -chdir=infra/terraform validate
```

Expected: PASS.

- [ ] **Step 3: Verify cluster access**

Run:

```bash
kubectl get nodes
kubectl get all -n vacciguard
```

Expected: PASS.

- [ ] **Step 4: Verify the baseline overlay renders without placeholders**

Run:

```bash
kubectl kustomize infra/kubernetes/baseline > /tmp/vacciguard-baseline.yaml
rg -n "REPLACE_ME_BUCKET|REPLACE_ME_REDIS_HOST" /tmp/vacciguard-baseline.yaml
```

Expected: no placeholder matches.

- [ ] **Step 5: Inspect the working tree**

Run:

```bash
git status --short
```

Expected: only the intended AWS apply-pass changes and any already-known unrelated local changes are present.

- [ ] **Step 6: Commit**

```bash
git add infra/terraform infra/kubernetes README.md docs/aws-baseline-foundation.md services/stream-processor/job.py tests/stream/test_job.py
git commit -m "feat: deploy aws baseline foundation"
```
