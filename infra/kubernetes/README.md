# Kubernetes Deployment Layout

- `base`: shared namespace and common configuration
- `aayush-baseline`: aayush-specific AWS baseline overlay
- `baseline`: fixed-capacity deployment manifests and overlays
- `tanish-baseline`: tanish-specific AWS baseline overlay
- `optimized`: reserved for autoscaling and tuning work
- monitoring resources live under `infra/monitoring/`

Validate with:

```bash
kubectl kustomize infra/kubernetes/base
kubectl kustomize infra/kubernetes/aayush-baseline
kubectl kustomize infra/kubernetes/baseline
kubectl kustomize infra/kubernetes/tanish-baseline
kubectl kustomize infra/kubernetes/optimized
```
