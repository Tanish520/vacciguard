# Dockerfile — VacciGuard PyFlink Pipeline
#
# Base: eclipse-temurin:17-jre-jammy
#   - Java 17 pre-installed — no slow apt-get install of openjdk
#   - Cuts build time from 40 minutes to under 5 minutes
#   - Works on both Apple Silicon (ARM64) and Intel/AMD (x86)
#
# Build : docker compose build
# Run   : docker compose up

FROM --platform=linux/amd64 eclipse-temurin:17-jdk-jammy

# ── System dependencies ────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-dev \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/bin/python3 /usr/bin/python

# ── Kinesis connector JAR (fat JAR — includes AWS SDK, no extra deps needed) ──
RUN mkdir -p /app/lib
COPY lib/flink-sql-connector-kinesis-4.3.0-1.18.jar /app/lib/

# ── Python dependencies ────────────────────────────────────────────────────
WORKDIR /app
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# ── Application code ───────────────────────────────────────────────────────
COPY . .

CMD ["python", "flink/pipeline.py"]