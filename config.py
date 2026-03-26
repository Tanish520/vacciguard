# config.py
# Shared constants for the VacciGuard pipeline.
# All scripts must import from here — never hardcode these values elsewhere.

KINESIS_STREAM_NAME = "vacciguard-stream"
DYNAMO_TABLE_NAME   = "VacciguardFridgeState"
REGION              = "ap-south-1"
