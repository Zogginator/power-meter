from __future__ import annotations

import os
import logging

from pathlib import Path
from datetime import datetime, timedelta, timezone

from influxdb_client.client.influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from ingest.config import INFLUX_URL, INFLUX_ORG, INFLUX_BUCKET, INFLUX_MEASUREMENT, INFLUX_TOKEN


log = logging.getLogger(__name__)

GRANULARITY_MAP = {
    1: "15m",
    2: "1d",
    3: "1mo",
}

def last_ts_with_data () -> datetime | None:
    query = f"""
    from(bucket: "{INFLUX_BUCKET}") 
        |> range(start: 0) 
        |> last()
    """ 
    tables= db_query(query)
    try:
        return max(r.get_time() for t in tables for r in t.records)
    except ValueError:
        return None
    

def daily_datapoints(dt : datetime):
   
    start_day = dt.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    stop_day = start_day + timedelta(days=1)
    
    query = f"""
    from(bucket: "{INFLUX_BUCKET}") 
        |> range(start: {flux_time(start_day)}, stop:  {flux_time(stop_day)})
        |> filter(fn: (r) =>
            r._measurement == "{INFLUX_MEASUREMENT}")
        |> keep(columns: ["_time"])
        |> unique(column: "_time")
    """
    
    tables = db_query(query)
    ts_count= sum(len(t.records) for t in tables)
    return ts_count
    
def flux_time(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def db_query(query):   
    with InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG) as client:
        
        query_api = client.query_api()
        try:
            tables = query_api.query(query)
            log.info("Read done")
            return(tables)
        except Exception as e:
            log.exception("Influx read failed")
            raise