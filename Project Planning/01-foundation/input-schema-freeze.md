# VacciGuard Input Schema Freeze

## Purpose
This document freezes the initial input schema for the three project datasets so Alok can start
large-scale data generation without causing downstream confusion for Aayush, Monty, and Tanish.

This schema should be treated as the shared contract for the first implementation phase.

## Freeze Rule
After Alok starts generating large workload files, field names and required columns should not be
changed casually. Any schema change should be discussed with:
- Tanish, because schema changes affect evaluation fairness and repo integration
- Aayush, because schema changes affect Spark validation, enrichment, and joins
- Monty, because schema changes affect deployment config, metrics labels, and test setup

## Global Data Rules
- use `snake_case` for all field names
- use UTC timestamps in ISO 8601 format such as `2026-04-05T10:15:00Z`
- use Celsius for all temperature values
- use booleans as `true` or `false`
- keep `event_id` unique in normal workloads
- only the `duplicate-event` workload may intentionally repeat `event_id`
- keep `device_id` stable across all datasets so joins remain simple
- keep `facility_id` stable across lookup and batch datasets

## Dataset 1: Live Telemetry Events

### File Format
- newline-delimited JSON for workload replay files
- storage location:
  - [data/workloads/dev/](/Users/tanishgupta/Desktop/vacciguard/data/workloads/dev)
  - [data/workloads/main/](/Users/tanishgupta/Desktop/vacciguard/data/workloads/main)
  - [data/workloads/heavy/](/Users/tanishgupta/Desktop/vacciguard/data/workloads/heavy)

### Schema
| Field | Type | Required | Example | Notes |
| --- | --- | --- | --- | --- |
| `event_id` | string | yes | `evt-000001` | unique per event except duplicate test workload |
| `device_id` | string | yes | `FR-0102` | joins to lookup dataset |
| `event_time` | timestamp string | yes | `2026-04-05T10:00:03Z` | event creation time in UTC |
| `temperature_c` | float | yes | `9.1` | sensor reading in Celsius |
| `door_open` | boolean | yes | `true` | fridge door state |
| `battery_pct` | integer | yes | `67` | expected range `0` to `100` |
| `location_lat` | float | no | `26.9124` | optional enrichment-friendly location |
| `location_lon` | float | no | `75.7873` | optional enrichment-friendly location |

### Validation Rules
- `event_id` must not be empty
- `device_id` must exist in the lookup dataset
- `battery_pct` must be between `0` and `100`
- `event_time` must be parseable as a UTC timestamp
- `temperature_c` should stay within a realistic synthetic range such as `-20` to `30`

### Example
```json
{
  "event_id": "evt-000001",
  "device_id": "FR-0102",
  "event_time": "2026-04-05T10:00:03Z",
  "temperature_c": 9.1,
  "door_open": true,
  "battery_pct": 67,
  "location_lat": 26.9124,
  "location_lon": 75.7873
}
```

## Dataset 2: Device and Facility Lookup Data

### File Format
- CSV
- storage location:
  - [data/reference/](/Users/tanishgupta/Desktop/vacciguard/data/reference)

### Schema
| Field | Type | Required | Example | Notes |
| --- | --- | --- | --- | --- |
| `device_id` | string | yes | `FR-0102` | primary join key from stream |
| `facility_id` | string | yes | `FAC-0002` | stable facility identifier |
| `facility_name` | string | yes | `Clinic B` | human-readable name |
| `district` | string | yes | `Udaipur` | reporting dimension |
| `state` | string | yes | `Rajasthan` | reporting dimension |
| `min_temp_c` | float | yes | `2.0` | lower safe threshold |
| `max_temp_c` | float | yes | `8.0` | upper safe threshold |
| `storage_type` | string | no | `refrigerator` | optional descriptive attribute |

### Validation Rules
- `device_id` must be unique in this file
- `facility_id` must not be empty
- `min_temp_c` must be less than `max_temp_c`
- district and state should use consistent spelling across all rows

### Example
```csv
device_id,facility_id,facility_name,district,state,min_temp_c,max_temp_c,storage_type
FR-0101,FAC-0001,Clinic A,Jaipur,Rajasthan,2,8,refrigerator
FR-0102,FAC-0002,Clinic B,Udaipur,Rajasthan,2,8,refrigerator
FR-0103,FAC-0003,Clinic C,Kota,Rajasthan,2,8,refrigerator
```

## Dataset 3: Daily Operations or Maintenance Logs

### File Format
- CSV
- storage location:
  - [data/batch/](/Users/tanishgupta/Desktop/vacciguard/data/batch)

### Schema
| Field | Type | Required | Example | Notes |
| --- | --- | --- | --- | --- |
| `log_date` | date string | yes | `2026-04-05` | daily batch date |
| `facility_id` | string | yes | `FAC-0002` | join key to lookup dataset |
| `power_outage_minutes` | integer | yes | `35` | total outage time for the day |
| `manual_temp_check_count` | integer | yes | `2` | number of manual checks |
| `maintenance_flag` | boolean | yes | `true` | whether maintenance occurred |
| `technician_visit_flag` | boolean | yes | `true` | whether technician visited |
| `stock_transfer_flag` | boolean | yes | `false` | whether stock movement happened |

### Validation Rules
- `facility_id` must exist in the lookup dataset
- `log_date` must use `YYYY-MM-DD`
- numeric fields should not be negative
- boolean flags must use `true` or `false`

### Example
```csv
log_date,facility_id,power_outage_minutes,manual_temp_check_count,maintenance_flag,technician_visit_flag,stock_transfer_flag
2026-04-05,FAC-0001,0,4,false,false,false
2026-04-05,FAC-0002,35,2,true,true,true
2026-04-05,FAC-0003,0,4,false,false,false
```

## Starter Template Files
The following starter files have been added so Alok can begin immediately:

- [data/workloads/dev/live-telemetry-sample.ndjson](/Users/tanishgupta/Desktop/vacciguard/data/workloads/dev/live-telemetry-sample.ndjson)
- [data/reference/device-facility-lookup-template.csv](/Users/tanishgupta/Desktop/vacciguard/data/reference/device-facility-lookup-template.csv)
- [data/batch/daily-operations-log-template.csv](/Users/tanishgupta/Desktop/vacciguard/data/batch/daily-operations-log-template.csv)

## Shared Contract Summary
- Alok generates data using this schema
- Aayush consumes the same fields in Spark jobs
- Monty deploys and tests around the same contract
- Tanish uses the frozen schema to keep evaluation and integration consistent
