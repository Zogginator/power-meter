from datetime import datetime, timezone

from ingest.influx import EnergyPoint, write_points


def main() -> None:
    now = datetime.now(timezone.utc)
    pts = [
        EnergyPoint(
            ts=now,
            consumption_kwh=0.0,
            feedin_kwh=0.0,
            source="smoke",
            meter_id="demo",
        )
    ]
    write_points(pts)
    print("OK: wrote 1 point to InfluxDB")


if __name__ == "__main__":
    main()
