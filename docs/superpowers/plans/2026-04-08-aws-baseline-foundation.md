# AWS Baseline Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reviewed AWS baseline deployment scaffold for VacciGuard with Terraform foundation files, `base` / `baseline` / `optimized` Kubernetes structure, and starter manifests for Kafka, the stream processor, and the replay producer.

**Architecture:** Keep the current local application code mostly intact and add deployment-oriented configuration around it. The AWS baseline scaffold should define shared cloud resources in Terraform, define fixed-capacity baseline runtime manifests in Kubernetes, and introduce only the minimum app configuration changes needed to support S3- and ElastiCache-compatible deployment values.

**Tech Stack:** Terraform, AWS provider, Amazon EKS, Amazon S3, Amazon ElastiCache Redis, Kubernetes YAML, Python, Docker

---

### Task 1: Scaffold the Terraform foundation layout

**Files:**
- Modify: `infra/terraform/.gitkeep`
- Create: `infra/terraform/README.md`
- Create: `infra/terraform/providers.tf`
- Create: `infra/terraform/variables.tf`
- Create: `infra/terraform/locals.tf`
- Create: `infra/terraform/main.tf`
- Create: `infra/terraform/outputs.tf`
- Create: `infra/terraform/versions.tf`
- Create: `infra/terraform/terraform.tfvars.example`

- [ ] **Step 1: Write the failing Terraform formatting check**

Run:

```bash
terraform -chdir=infra/terraform fmt -check
```

Expected: FAIL because `infra/terraform` only contains `.gitkeep` and no Terraform files.

- [ ] **Step 2: Remove the placeholder file**

Run:

```bash
rm infra/terraform/.gitkeep
```

- [ ] **Step 3: Create the Terraform version lock file**

Create `infra/terraform/versions.tf` with:

```hcl
terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}
```

- [ ] **Step 4: Create the provider and default tag configuration**

Create `infra/terraform/providers.tf` with:

```hcl
provider "aws" {
  region = var.aws_region

  default_tags {
    tags = local.common_tags
  }
}
```

- [ ] **Step 5: Create the variable definitions**

Create `infra/terraform/variables.tf` with:

```hcl
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

variable "redis_node_type" {
  description = "ElastiCache node type for the baseline environment."
  type        = string
  default     = "cache.t4g.micro"
}
```

- [ ] **Step 6: Create the locals file**

Create `infra/terraform/locals.tf` with:

```hcl
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
```

- [ ] **Step 7: Create the main infrastructure scaffold**

Create `infra/terraform/main.tf` with:

```hcl
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
  vpc_id      = "vpc-REPLACE_ME"

  tags = {
    Name = "${local.name_prefix}-redis"
  }
}

resource "aws_elasticache_subnet_group" "redis" {
  name       = var.redis_subnet_group_name
  subnet_ids = ["subnet-REPLACE_ME_A", "subnet-REPLACE_ME_B"]
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
```

- [ ] **Step 8: Create the output definitions**

Create `infra/terraform/outputs.tf` with:

```hcl
output "eks_cluster_name" {
  value = local.eks_cluster_name
}

output "s3_bucket_name" {
  value = aws_s3_bucket.pipeline_data.bucket
}

output "redis_endpoint" {
  value = aws_elasticache_cluster.redis.cache_nodes[0].address
}
```

- [ ] **Step 9: Create the example tfvars file**

Create `infra/terraform/terraform.tfvars.example` with:

```hcl
project_name               = "vacciguard"
environment_name           = "baseline"
aws_region                 = "ap-south-1"
eks_cluster_name_override  = ""
s3_bucket_name_override    = ""
redis_subnet_group_name    = "vacciguard-baseline-redis-subnets"
redis_node_type            = "cache.t4g.micro"
```

- [ ] **Step 10: Create the Terraform README**

Create `infra/terraform/README.md` with:

```md
# Terraform AWS Baseline Foundation

This directory holds the first AWS baseline scaffold for VacciGuard.

## Scope

- provider and version configuration
- naming and tag conventions
- baseline S3 bucket scaffold
- baseline ElastiCache Redis scaffold
- environment variables and outputs for later workload wiring

## Not Included Yet

- real VPC module wiring
- real EKS module wiring
- apply-ready subnet and security group values
- monitoring resources

## Validation

```bash
terraform -chdir=infra/terraform fmt -check
terraform -chdir=infra/terraform validate
```
```

- [ ] **Step 11: Run Terraform formatting**

Run:

```bash
terraform -chdir=infra/terraform fmt
terraform -chdir=infra/terraform fmt -check
```

Expected: PASS.

- [ ] **Step 12: Commit the Terraform scaffold**

```bash
git add infra/terraform
git commit -m "infra: add aws baseline terraform scaffold"
```

### Task 2: Create the Kubernetes base, baseline, and optimized structure

**Files:**
- Modify: `infra/kubernetes/base/.gitkeep`
- Modify: `infra/kubernetes/baseline/.gitkeep`
- Modify: `infra/kubernetes/optimized/.gitkeep`
- Create: `infra/kubernetes/README.md`
- Create: `infra/kubernetes/base/namespace.yaml`
- Create: `infra/kubernetes/base/configmap-pipeline.yaml`
- Create: `infra/kubernetes/base/kustomization.yaml`
- Create: `infra/kubernetes/baseline/kustomization.yaml`
- Create: `infra/kubernetes/optimized/kustomization.yaml`

- [ ] **Step 1: Write the failing directory structure check**

Run:

```bash
find infra/kubernetes -maxdepth 2 -type f | sort
```

Expected: only `.gitkeep` files are present.

- [ ] **Step 2: Remove the placeholder files**

Run:

```bash
rm infra/kubernetes/base/.gitkeep
rm infra/kubernetes/baseline/.gitkeep
rm infra/kubernetes/optimized/.gitkeep
```

- [ ] **Step 3: Create the shared namespace manifest**

Create `infra/kubernetes/base/namespace.yaml` with:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: vacciguard
```

- [ ] **Step 4: Create the shared pipeline config map**

Create `infra/kubernetes/base/configmap-pipeline.yaml` with:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: vacciguard-pipeline-config
  namespace: vacciguard
data:
  APP_NAME: vacciguard-stream-processor
  KAFKA_TOPIC: vacciguard-telemetry
  KAFKA_BOOTSTRAP_SERVERS: kafka:9092
  PROCESSED_OUTPUT_PATH: s3a://REPLACE_ME_BUCKET/processed
  INVALID_OUTPUT_PATH: s3a://REPLACE_ME_BUCKET/invalid
  CHECKPOINT_ROOT: s3a://REPLACE_ME_BUCKET/checkpoints
  BREACH_WINDOW_OUTPUT_PATH: s3a://REPLACE_ME_BUCKET/breach_windows
  REDIS_HOST: REPLACE_ME_REDIS_HOST
  REDIS_PORT: "6379"
  REDIS_DB: "0"
  KAFKA_STARTING_OFFSETS: earliest
  TRIGGER_INTERVAL: 5 seconds
  WATERMARK_DELAY: 10 minutes
```

- [ ] **Step 5: Create the base kustomization**

Create `infra/kubernetes/base/kustomization.yaml` with:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: vacciguard
resources:
  - namespace.yaml
  - configmap-pipeline.yaml
```

- [ ] **Step 6: Create the baseline kustomization**

Create `infra/kubernetes/baseline/kustomization.yaml` with:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../base
```

- [ ] **Step 7: Create the optimized placeholder kustomization**

Create `infra/kubernetes/optimized/kustomization.yaml` with:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../base
```

- [ ] **Step 8: Create the Kubernetes README**

Create `infra/kubernetes/README.md` with:

```md
# Kubernetes Deployment Layout

- `base`: shared namespace and common configuration
- `baseline`: fixed-capacity deployment manifests and overlays
- `optimized`: reserved for autoscaling and tuning work

Validate with:

```bash
kubectl kustomize infra/kubernetes/base
kubectl kustomize infra/kubernetes/baseline
kubectl kustomize infra/kubernetes/optimized
```
```

- [ ] **Step 9: Validate the kustomize structure**

Run:

```bash
kubectl kustomize infra/kubernetes/base > /tmp/vacciguard-base.yaml
kubectl kustomize infra/kubernetes/baseline > /tmp/vacciguard-baseline.yaml
kubectl kustomize infra/kubernetes/optimized > /tmp/vacciguard-optimized.yaml
```

Expected: all three commands succeed.

- [ ] **Step 10: Commit the deployment layout**

```bash
git add infra/kubernetes
git commit -m "infra: add kubernetes baseline layout"
```

### Task 3: Add starter baseline manifests for Kafka, stream processor, and replay producer

**Files:**
- Modify: `infra/kubernetes/base/kustomization.yaml`
- Modify: `infra/kubernetes/baseline/kustomization.yaml`
- Create: `infra/kubernetes/base/deployment-stream-processor.yaml`
- Create: `infra/kubernetes/base/job-replay-producer.yaml`
- Create: `infra/kubernetes/base/service-kafka.yaml`
- Create: `infra/kubernetes/base/statefulset-kafka.yaml`
- Create: `infra/kubernetes/baseline/patch-stream-processor-resources.yaml`
- Create: `infra/kubernetes/baseline/patch-kafka-resources.yaml`

- [ ] **Step 1: Write the failing manifest validation check**

Run:

```bash
kubectl kustomize infra/kubernetes/baseline | rg "stream-processor|replay-producer|kafka"
```

Expected: FAIL or return no matching resources because those manifests do not exist yet.

- [ ] **Step 2: Create the Kafka headless service**

Create `infra/kubernetes/base/service-kafka.yaml` with:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: kafka
  namespace: vacciguard
spec:
  clusterIP: None
  selector:
    app: kafka
  ports:
    - name: kafka
      port: 9092
      targetPort: 9092
```

- [ ] **Step 3: Create the Kafka stateful set**

Create `infra/kubernetes/base/statefulset-kafka.yaml` with:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kafka
  namespace: vacciguard
spec:
  serviceName: kafka
  replicas: 1
  selector:
    matchLabels:
      app: kafka
  template:
    metadata:
      labels:
        app: kafka
    spec:
      containers:
        - name: kafka
          image: bitnamilegacy/kafka:4.0.0-debian-12-r10
          env:
            - name: KAFKA_CFG_NODE_ID
              value: "1"
            - name: KAFKA_CFG_PROCESS_ROLES
              value: "broker,controller"
            - name: KAFKA_CFG_CONTROLLER_QUORUM_VOTERS
              value: "1@kafka-0.kafka.vacciguard.svc.cluster.local:9093"
            - name: KAFKA_CFG_LISTENERS
              value: "PLAINTEXT://:9092,CONTROLLER://:9093"
            - name: KAFKA_CFG_ADVERTISED_LISTENERS
              value: "PLAINTEXT://kafka-0.kafka.vacciguard.svc.cluster.local:9092"
            - name: KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP
              value: "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT"
            - name: KAFKA_CFG_CONTROLLER_LISTENER_NAMES
              value: "CONTROLLER"
            - name: KAFKA_CFG_INTER_BROKER_LISTENER_NAME
              value: "PLAINTEXT"
            - name: ALLOW_PLAINTEXT_LISTENER
              value: "yes"
          ports:
            - containerPort: 9092
            - containerPort: 9093
```

- [ ] **Step 4: Create the stream processor deployment**

Create `infra/kubernetes/base/deployment-stream-processor.yaml` with:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: stream-processor
  namespace: vacciguard
spec:
  replicas: 1
  selector:
    matchLabels:
      app: stream-processor
  template:
    metadata:
      labels:
        app: stream-processor
    spec:
      containers:
        - name: stream-processor
          image: vacciguard-stream-processor:baseline
          envFrom:
            - configMapRef:
                name: vacciguard-pipeline-config
```

- [ ] **Step 5: Create the replay producer job**

Create `infra/kubernetes/base/job-replay-producer.yaml` with:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: replay-producer
  namespace: vacciguard
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: replay-producer
          image: vacciguard-replay-producer:baseline
          env:
            - name: KAFKA_BOOTSTRAP_SERVERS
              value: kafka:9092
            - name: KAFKA_TOPIC
              value: vacciguard-telemetry
            - name: WORKLOAD_FILE
              value: /data/workloads/dev/events.ndjson
            - name: EVENTS_PER_SECOND
              value: "5"
            - name: LOOP
              value: "false"
```

- [ ] **Step 6: Register the new base resources**

Update `infra/kubernetes/base/kustomization.yaml` to:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: vacciguard
resources:
  - namespace.yaml
  - configmap-pipeline.yaml
  - service-kafka.yaml
  - statefulset-kafka.yaml
  - deployment-stream-processor.yaml
  - job-replay-producer.yaml
```

- [ ] **Step 7: Create the baseline Kafka resource patch**

Create `infra/kubernetes/baseline/patch-kafka-resources.yaml` with:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kafka
  namespace: vacciguard
spec:
  template:
    spec:
      containers:
        - name: kafka
          resources:
            requests:
              cpu: "250m"
              memory: "512Mi"
            limits:
              cpu: "500m"
              memory: "1Gi"
```

- [ ] **Step 8: Create the baseline stream processor resource patch**

Create `infra/kubernetes/baseline/patch-stream-processor-resources.yaml` with:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: stream-processor
  namespace: vacciguard
spec:
  replicas: 1
  template:
    spec:
      containers:
        - name: stream-processor
          resources:
            requests:
              cpu: "500m"
              memory: "1Gi"
            limits:
              cpu: "1000m"
              memory: "2Gi"
```

- [ ] **Step 9: Register the baseline patches**

Update `infra/kubernetes/baseline/kustomization.yaml` to:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../base
patchesStrategicMerge:
  - patch-kafka-resources.yaml
  - patch-stream-processor-resources.yaml
```

- [ ] **Step 10: Validate the baseline manifest output**

Run:

```bash
kubectl kustomize infra/kubernetes/baseline | rg "kind: StatefulSet|kind: Deployment|kind: Job|name: kafka|name: stream-processor|name: replay-producer"
```

Expected: output contains the Kafka StatefulSet, stream-processor Deployment, and replay-producer Job.

- [ ] **Step 11: Commit the starter manifests**

```bash
git add infra/kubernetes/base infra/kubernetes/baseline
git commit -m "infra: add baseline workload manifests"
```

### Task 4: Make the stream processor deployment-configurable for AWS-style paths

**Files:**
- Modify: `services/stream-processor/job.py`
- Modify: `tests/stream/test_job.py`

- [ ] **Step 1: Write the failing configuration test**

Append to `tests/stream/test_job.py`:

```python
class OutputPathConfigTests(unittest.TestCase):
    def test_breach_window_output_path_defaults_are_exposed(self):
        self.assertEqual(stream_job.BREACH_WINDOW_OUTPUT_PATH, "/data/output/breach_windows")
```

Run:

```bash
python3 -m unittest tests.stream.test_job
```

Expected: PASS if the constant already exists. If it already passes, immediately add the next test:

```python
    def test_build_output_streams_accepts_non_local_watermark_config(self):
        self.assertEqual(stream_job.WATERMARK_DELAY, "10 minutes")
```

Expected: PASS. This task is only complete after verifying the existing config contract still passes under test.

- [ ] **Step 2: Add a deployment documentation comment to the config block**

In `services/stream-processor/job.py`, keep the existing constants and add this comment above the output-path constants:

```python
# These environment-driven paths allow the same processor code to run locally
# against mounted files or in AWS-oriented environments with S3-compatible paths.
```

- [ ] **Step 3: Run the stream unit tests**

Run:

```bash
python3 -m unittest tests.stream.test_job
```

Expected: PASS.

- [ ] **Step 4: Commit the configuration-contract clarification**

```bash
git add services/stream-processor/job.py tests/stream/test_job.py
git commit -m "docs: clarify aws deployment config contract"
```

### Task 5: Document the baseline deployment workflow

**Files:**
- Modify: `README.md`
- Create: `docs/aws-baseline-foundation.md`

- [ ] **Step 1: Write the failing documentation discoverability check**

Run:

```bash
rg -n "AWS Baseline|terraform -chdir=infra/terraform|kubectl kustomize infra/kubernetes/baseline" README.md docs
```

Expected: FAIL or return no AWS baseline deployment guide.

- [ ] **Step 2: Create the dedicated AWS baseline guide**

Create `docs/aws-baseline-foundation.md` with:

```md
# AWS Baseline Foundation

## What This Includes

- Terraform scaffold for the first AWS baseline environment
- Kubernetes `base`, `baseline`, and `optimized` layout
- starter manifests for Kafka, stream processor, and replay producer

## What This Does Not Include Yet

- applying AWS resources
- monitoring stack rollout
- optimized deployment behavior

## Validate The Scaffold

```bash
terraform -chdir=infra/terraform fmt -check
kubectl kustomize infra/kubernetes/base > /tmp/vacciguard-base.yaml
kubectl kustomize infra/kubernetes/baseline > /tmp/vacciguard-baseline.yaml
kubectl kustomize infra/kubernetes/optimized > /tmp/vacciguard-optimized.yaml
python3 -m unittest tests.stream.test_job
```
```

- [ ] **Step 3: Add an AWS baseline section to the README**

Append this section to `README.md`:

```md
## AWS Baseline Foundation

The repository includes an AWS baseline deployment scaffold under `infra/terraform` and `infra/kubernetes`.

Validate it with:

```bash
terraform -chdir=infra/terraform fmt -check
kubectl kustomize infra/kubernetes/base
kubectl kustomize infra/kubernetes/baseline
kubectl kustomize infra/kubernetes/optimized
```

See `docs/aws-baseline-foundation.md` for the baseline deployment structure and current boundaries.
```

- [ ] **Step 4: Verify the documentation is discoverable**

Run:

```bash
rg -n "AWS Baseline Foundation|terraform -chdir=infra/terraform|docs/aws-baseline-foundation.md" README.md docs
```

Expected: PASS with matches in both `README.md` and `docs/aws-baseline-foundation.md`.

- [ ] **Step 5: Commit the deployment docs**

```bash
git add README.md docs/aws-baseline-foundation.md
git commit -m "docs: add aws baseline foundation guide"
```

### Task 6: Final verification pass

**Files:**
- Modify: any of the files above only if verification reveals a real defect

- [ ] **Step 1: Run Terraform formatting**

Run:

```bash
terraform -chdir=infra/terraform fmt -check
```

Expected: PASS.

- [ ] **Step 2: Render all Kubernetes layouts**

Run:

```bash
kubectl kustomize infra/kubernetes/base > /tmp/vacciguard-base.yaml
kubectl kustomize infra/kubernetes/baseline > /tmp/vacciguard-baseline.yaml
kubectl kustomize infra/kubernetes/optimized > /tmp/vacciguard-optimized.yaml
```

Expected: PASS.

- [ ] **Step 3: Run the stream unit tests**

Run:

```bash
python3 -m unittest tests.stream.test_job
```

Expected: PASS.

- [ ] **Step 4: Inspect the working tree**

Run:

```bash
git status --short
```

Expected: only the intended AWS foundation and any already-known in-progress local changes are present.

- [ ] **Step 5: Commit the completed AWS baseline scaffold**

```bash
git add infra/terraform infra/kubernetes README.md docs/aws-baseline-foundation.md services/stream-processor/job.py tests/stream/test_job.py
git commit -m "feat: add aws baseline foundation scaffold"
```
