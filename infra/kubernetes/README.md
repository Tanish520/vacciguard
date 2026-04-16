# Kubernetes Deployment Layout

- `base`: shared namespace and common configuration
- `baseline`: fixed-capacity deployment manifests and overlays
- `optimized`: reserved for autoscaling and tuning work
- `aws-observability`: managed AMP / Grafana / CloudWatch collector bundle for AWS runs
- monitoring resources live under `infra/monitoring/`

Validate with:

```bash
kubectl kustomize infra/kubernetes/base
kubectl kustomize infra/kubernetes/baseline
kubectl kustomize infra/kubernetes/optimized
kubectl kustomize infra/kubernetes/aws-observability
```
