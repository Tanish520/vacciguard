# Data Quality

## Requirement
The brief explicitly asks for data validation and deduplication.

## What This Means
The pipeline should not blindly trust every incoming record. It should check whether data is valid, detect duplicates, and keep bad records visible for analysis instead of silently losing them.

## How The Architecture Addresses It
### Validation
The processing logic should validate:

- required fields such as `event_id`, `device_id`, and `event_time`
- correct field types
- valid temperature ranges
- valid battery percentage range
- valid device reference matches when needed

### Deduplication
The pipeline should use `event_id` as the main uniqueness key.

If duplicates arrive because of retries or replay:

- repeated `event_id` values are detected
- duplicate downstream effects are suppressed
- breaches and alerts are not double-counted

### Bad Data Handling
Rejected records should go to a visible quarantine path such as:

- a Kafka dead-letter topic
- an S3 rejected-record folder

## How We Will Measure It
We will record:

- total input records
- valid output records
- invalid record count
- duplicate record count
- late record count
- percentage of clean records

## What We Can Honestly Claim
If the pipeline consistently validates records and suppresses duplicate effects during normal and failure scenarios, we can claim that it addresses the data quality requirement in a measurable way.
