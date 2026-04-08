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
```

## EKS Access

Operator guardrail: confirm the AWS account before touching kubeconfig.

```bash
aws sts get-caller-identity
```

```bash
aws eks update-kubeconfig --region ap-south-1 --name "$(terraform -chdir=infra/terraform output -raw eks_cluster_name)"
kubectl get nodes
```

Observed in this pass:

- `aws eks update-kubeconfig` succeeded for AWS account `347038623570` and cluster `vacciguard-baseline-eks`.
- `kubectl get nodes` showed 2 nodes, both in `Ready` state.

## Live Value Injection

Use Terraform outputs to copy the live values into `infra/kubernetes/baseline/configmap-pipeline.yaml`:

```bash
terraform -chdir=infra/terraform output -raw s3_bucket_name
terraform -chdir=infra/terraform output -raw redis_endpoint
```

After copying those values into the baseline configmap, run a render check before applying:

```bash
kubectl kustomize infra/kubernetes/baseline > /tmp/vacciguard-baseline.yaml
rg -n "REPLACE_ME_BUCKET|REPLACE_ME_REDIS_HOST" /tmp/vacciguard-baseline.yaml
kubectl apply -k infra/kubernetes/baseline
```

## Repeatable Evaluation Run

Use the AWS evaluation script to give each run its own Kafka topic and S3 prefix, then save a local report with the result evidence:

```bash
bash scripts/run-aws-baseline-evaluation.sh
```

Optional: pass an explicit run id if you want a stable label for the output prefix and report file.

```bash
bash scripts/run-aws-baseline-evaluation.sh demo-run-001
```

Each run writes:

- processed output under `s3://vacciguard-baseline-data/evaluations/<run-id>/processed/`
- invalid output under `s3://vacciguard-baseline-data/evaluations/<run-id>/invalid/`
- breach window output under `s3://vacciguard-baseline-data/evaluations/<run-id>/breach_windows/`
- checkpoints under `s3://vacciguard-baseline-data/evaluations/<run-id>/checkpoints/`
- a local markdown report under `artifacts/aws-baseline-evaluations/<run-id>.md`

The report now includes an evaluation summary table for:

- average end-to-end latency
- P95 latency
- throughput
- input events
- processed events
- invalid events
- deduplicated events
- breach events

The same report also keeps placeholders for:

- 10x spike success/failure
- recovery time after failure
- cost per run
- cost per GB processed

Those four metrics stay as `Not run` until you execute dedicated spike, failure, and cost collection scenarios. That means the report is already useful for SLA-style evidence around latency and steady-state correctness, but it becomes a true SLA scorecard only when you attach target thresholds such as `P95 latency < 5 seconds` and `recovery time < 2 minutes`.
