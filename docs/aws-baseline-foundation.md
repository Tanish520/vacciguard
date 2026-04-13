# AWS Baseline Foundation

## What This Includes

- Terraform scaffold for the first AWS baseline environment
- Kubernetes `base`, `baseline`, and `optimized` layout
- starter manifests for Kafka, stream processor, and replay producer

## What This Does Not Include Yet

- applying AWS resources
- optimized deployment behavior

## Monitoring Stack

Monitoring resources are split across `infra/monitoring/cloudwatch`, `infra/monitoring/prometheus`, and `infra/monitoring/grafana`.

CloudWatch and Container Insights provide the AWS-facing observability layer, while Prometheus and Grafana provide the baseline runtime and evaluation visibility we use to inspect the platform during runs.

Before merging changes to the monitoring stack, render the Prometheus and Grafana bundles to confirm the kustomizations still resolve cleanly:

```bash
kubectl kustomize infra/monitoring/prometheus > /tmp/vacciguard-prometheus.yaml
kubectl kustomize infra/monitoring/grafana > /tmp/vacciguard-grafana.yaml
```

To deploy the in-cluster monitoring stack on the baseline cluster:

```bash
kubectl apply -k infra/monitoring/prometheus
kubectl apply -k infra/monitoring/grafana
```

To inspect the UI locally:

```bash
kubectl port-forward -n monitoring svc/prometheus 9090:9090
kubectl port-forward -n monitoring svc/grafana 3000:3000
```

The baseline pipeline now exposes Prometheus text metrics directly from the application containers:

- stream processor: `http://<stream-pod>:9108/metrics`
- replay producer: `http://<replay-pod>:9109/metrics`

Prometheus scrapes only those explicit endpoints:

- `stream-processor-metrics`
- `replay-producer-metrics`

The first operational dashboard, `VacciGuard Baseline Overview`, is backed by those live metrics and shows:

- replay sent events
- replay completion status
- stream processed, invalid, deduplicated, and breach totals
- latest batch average latency
- latest batch P95 latency

Important caveat: replay producer metrics are job-scoped. The replay completion panel is reliable while the replay pod is still present, but those metrics disappear when Kubernetes garbage-collects the finished job pod. The baseline job keeps a short drain window through `REPLAY_METRICS_DRAIN_SECONDS` so Prometheus and Grafana have time to scrape the final completion metrics before the pod exits.

## AWS-Native Evaluation

Use the in-cluster evaluation controller to run AWS-native experiments for either pipeline target:

```bash
bash scripts/run-aws-evaluation-controller.sh baseline normal
bash scripts/run-aws-evaluation-controller.sh optimized spike
```

Each run stores its evidence under:

- `s3://<bucket>/evaluations/<pipeline-target>/<scenario>/<run-id>/`

with:

- `processed/`
- `invalid/`
- `breach_windows/`
- `checkpoints/`
- `report.md`
- `report.json`

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
