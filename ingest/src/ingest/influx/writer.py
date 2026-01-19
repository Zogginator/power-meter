import os
import logging

from pathlib import Path
from datetime import datetime, timezone

from ingest.logging_setup import setup_console
from ingest.eon.client import EonClient, EonQuery, TokenStore, MeasurementSeries, MeasurementPoint

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from ingest.config import INFLUX_URL, INFLUX_ORG, INFLUX_BUCKET, INFLUX_MEASUREMENT,INFLUX_TOKEN


log = logging.getLogger(__name__)

GRANULARITY_MAP = {
    1: "15m",
    2: "1d",
    3: "1mo",
}


def write_series(ser: MeasurementSeries) -> None:
    # if not check_consistency(ser):            #general consisteny check to be built
    #    return

    
    record: list[Point] = []
    for p in ser.points:
        pt = Point(INFLUX_MEASUREMENT)          #Measurement id - hardwired no other type of measurement types from this meter
        pt.tag("source", ser.source)            #API, mail, csv_backfill, etc. ...
        pt.tag("meter_id", ser.pod_id)          #Only one meter for the time being. If more -> refactor
        pt.tag("granularity", GRANULARITY_MAP[ser.interval]) 
        pt.time(p.timestamp, WritePrecision.S)  #UTC epoch tmestamp
        for field, value in p.values.items():    #itarete values in dictionary
            if value is not None:
                pt.field(field, value)

        record.append(pt)
    
   

    with InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)     # SYNCHRONOUS -> wait until write finished
        try:
            log.info("Writing %d points to Influx measurements", len(record))
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=record, write_precision=WritePrecision.S)
            log.info("Write done")
        except Exception as e:
            log.exception("Influx write failed")
            raise
    



