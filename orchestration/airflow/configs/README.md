This folder holds Airflow runtime notes for the VacciGuard batch analytics DAG.

## Manual batch analytics runbook

The `vacciguard_batch_analytics` DAG is manual-trigger only. It expects the repository to be mounted at `/workspace/vacciguard` inside the Airflow worker image, and the worker must have AWS credentials plus S3 access because the job reads archived inputs from S3 and writes summary outputs back to S3.

Trigger the DAG after an optimized evaluation run has finished and its archived outputs are available under a single prefix. The batch job expects these input prefixes:

- `processed_input`: archived Parquet output for processed events
- `invalid_input`: archived JSON lines output for invalid events
- `breach_windows_input`: archived JSON lines output for breach windows

Example manual trigger parameters for a completed optimized run:

```text
processed_input: s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/normal/opt-normal-3-20260417t184552z/processed/
invalid_input: s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/normal/opt-normal-3-20260417t184552z/invalid/
breach_windows_input: s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/normal/opt-normal-3-20260417t184552z/breach_windows/
compliance_output: s3://vacciguard-tanish-baseline-ap-south-1-data/batch-analytics/demo/daily_compliance_summary/
audit_output: s3://vacciguard-tanish-baseline-ap-south-1-data/batch-analytics/demo/daily_audit_summary/
```

When the run completes, expect these S3-style output prefixes to contain the generated Parquet summaries:

- `compliance_output/summary.parquet`
- `audit_output/summary.parquet`

Use matching output prefixes for each run so the compliance and audit summaries stay grouped by batch execution.
