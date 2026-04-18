This folder holds Airflow runtime notes for the VacciGuard batch analytics DAG.
The DAG is manual-trigger only and expects the repository to be mounted at /workspace/vacciguard inside the Airflow worker image.
The Airflow worker also needs AWS credentials and S3 access because the batch job reads archived inputs from S3 and writes compliance and audit outputs back to S3.
