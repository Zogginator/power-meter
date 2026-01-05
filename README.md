\# Energy Pipeline (InfluxDB + Python ingest)



This project ingests smart meter energy data into InfluxDB and supports:

\- periodic polling (e.g. 15-minute data)

\- historical backfill (monthly batches)

\- CSV imports (manual exports and email attachments)



\## Quick start (Docker)

1\. Copy env template:

&nbsp;  - `cp .env.example .env` and fill values

2\. Start services:

&nbsp;  - `docker compose up -d --build`

3\. Influx UI:

&nbsp;  - via SSH tunnel recommended: `ssh -L 8086:localhost:8086 <server-alias>`

&nbsp;  - open `http://localhost:8086`



