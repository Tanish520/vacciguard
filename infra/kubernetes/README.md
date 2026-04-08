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
