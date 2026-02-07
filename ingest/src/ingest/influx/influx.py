from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from influxdb_client.client.influxdb_client import InfluxDBClient
from influxdb_client.client.write.point import Point
from influxdb_client.domain.write_precision import WritePrecision
from ingest.config import INFLUX_URL, INFLUX_ORG, INFLUX_BUCKET, INFLUX_TOKEN
import logging

log = logging.getLogger(__name__)

@dataclass(frozen=True)
class EnergyPoint:
    ts: datetime  # should be timezone-aware
    consumption_kwh: float | None = None
    feedin_kwh: float | None = None
    source: str = "unknown"
    meter_id: str | None = None


def write_points(points: list[EnergyPoint]) -> None:
    if not points:
        return

    # Normalize timestamps to UTC
    normalized: list[Point] = []
    for p in points:
        ts = p.ts
        if ts.tzinfo is None:
            # refuse naive datetimes (silent bugs)
            raise ValueError("EnergyPoint.ts must be timezone-aware")
        ts_utc = ts.astimezone(timezone.utc)

        pt = Point("energy_15m").time(ts_utc, WritePrecision.S)
        pt.tag("source", p.source)
        if p.meter_id:
            pt.tag("meter_id", p.meter_id)

        if p.consumption_kwh is not None:
            pt.field("consumption_kwh", float(p.consumption_kwh))
        if p.feedin_kwh is not None:
            pt.field("feedin_kwh", float(p.feedin_kwh))

        normalized.append(pt)

    with InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG) as client:
        write_api = client.write_api()
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=normalized)
