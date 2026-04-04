# VacciGuard Dataset Plan

## Goal
Use the smallest possible set of datasets that still satisfies the assignment requirements:

- multiple data sources
- stream and batch processing
- joins and enrichment
- reporting and analytics
- baseline vs optimized evaluation

The project should use exactly three logical datasets.

## Dataset 1: Live Telemetry Events
### Role
This is the main streaming dataset. It represents readings coming continuously from simulated cold-chain devices.

### Type
- Synthetic
- Streaming
- JSON or CSV events

### Why It Exists
- drives the real-time pipeline
- supports breach detection
- supports latency and throughput experiments
- gives you something to scale and stress-test

### Recommended Fields
- `event_id`
- `device_id`
- `event_time`
- `temperature_c`
- `door_open`
- `battery_pct`
- `location_lat`
- `location_lon`

### Example
```json
{
  "event_id": "evt-0001",
  "device_id": "FR-102",
  "event_time": "2026-04-04T10:00:03Z",
  "temperature_c": 9.1,
  "door_open": true,
  "battery_pct": 67,
  "location_lat": 26.9124,
  "location_lon": 75.7873
}
```

### What You Can Do With It
- detect temperature breaches
- measure end-to-end delay
- measure throughput under normal and burst load
- test late-arriving and duplicate events

## Dataset 2: Device And Facility Lookup Data
### Role
This is the small supporting dataset that gives meaning to the live events.

### Type
- Small lookup table
- Batch file
- CSV is enough

### Why It Exists
- supports joins and enrichment
- helps classify whether a reading is safe or unsafe
- enables reporting by facility, district, and state
- keeps the project aligned with the assignment requirement for enrichment and analytics

### Recommended Fields
- `device_id`
- `facility_id`
- `facility_name`
- `district`
- `state`
- `min_temp_c`
- `max_temp_c`
- `storage_type`

### Example
```csv
device_id,facility_id,facility_name,district,state,min_temp_c,max_temp_c,storage_type
FR-101,FAC-001,Clinic A,Jaipur,Rajasthan,2,8,refrigerator
FR-102,FAC-002,Clinic B,Udaipur,Rajasthan,2,8,refrigerator
FR-103,FAC-003,Clinic C,Kota,Rajasthan,2,8,refrigerator
```

### What You Can Do With It
- convert `FR-102` into a real facility name
- check if `9.1C` is a breach for that device
- aggregate breaches by district or state
- demonstrate stream-to-reference joins

### Important Note
This is not a second live stream. It is just a small helper file or table.

## Dataset 3: Daily Operations Or Maintenance Logs
### Role
This is the independent batch dataset. It represents non-streaming operational information uploaded once per day or once per shift.

### Type
- Synthetic at first
- Batch upload
- CSV or Parquet

### Why It Exists
- gives you a true batch path
- supports hybrid pipeline design
- lets you join operational context with telemetry results
- strengthens historical reporting and analytics

### Recommended Fields
- `log_date`
- `facility_id`
- `power_outage_minutes`
- `manual_temp_check_count`
- `maintenance_flag`
- `technician_visit_flag`
- `stock_transfer_flag`

### Example
```csv
log_date,facility_id,power_outage_minutes,manual_temp_check_count,maintenance_flag,technician_visit_flag,stock_transfer_flag
2026-04-04,FAC-001,0,4,false,false,false
2026-04-04,FAC-002,35,2,true,true,true
2026-04-04,FAC-003,0,4,false,false,false
```

### What You Can Do With It
- compare breach frequency with power outage time
- produce daily facility compliance reports
- build batch analytics separate from the stream path
- show that the pipeline handles both real-time and scheduled data

## Why These Three Are Enough
Together, these datasets cover the whole assignment without scope creep:

- `Dataset 1` gives you the event stream
- `Dataset 2` gives you enrichment and rule context
- `Dataset 3` gives you true batch ingestion and historical analysis

This is enough to demonstrate:

- multiple data sources
- hybrid stream + batch processing
- joins and enrichment
- hot and cold storage design
- observability and alerts
- controlled baseline vs optimized experiments

## How They Map To The Assignment
### Functional Requirements
- Event streams: `Dataset 1`
- Batch uploads: `Dataset 3`
- Enrichment and joins: `Dataset 2` joined with `Dataset 1`
- Analytics and reporting: all three together

### Evaluation Methodology
- Synthetic data generation: `Dataset 1` and `Dataset 3`
- Controlled experiments: vary the rate and pattern of `Dataset 1`
- Baseline vs optimized comparison: run the same three datasets through both versions
- Metrics: latency, throughput, cost, and recovery time

## What Not To Add
To stay aligned and avoid losing time, do not add:

- patient records
- vaccine inventory management
- appointment systems
- billing or payment data
- too many extra device attributes
- more than one independent batch dataset

## Recommended Simplification
If needed, keep the first version very small:

- `Dataset 1`: 10 to 50 devices at first
- `Dataset 2`: one CSV file with one row per device
- `Dataset 3`: one daily CSV file per date

You can scale volume later during evaluation without changing the schema.

## Note On Real Data
The assignment asks for synthetic and real datasets. The simplest way to satisfy that later is:

- keep `Dataset 1` synthetic
- keep `Dataset 3` synthetic
- seed `Dataset 2` partially from a real public geography or facility list if possible

If a fully suitable public vaccine-cold-chain dataset is hard to find, we can still keep the pipeline design stable and then decide the best real supporting dataset in a later phase.
