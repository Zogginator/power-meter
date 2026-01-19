import os
import logging
import json

from pathlib import Path
from datetime import datetime, time, timezone

from ingest.logging_setup import setup_console
from ingest.eon.client import EonClient, EonQuery, TokenStore, MeasurementSeries, MeasurementPoint
from ingest.influx.writer import write_series
from ingest.influx.reader import last_ts_with_data, daily_datapoints



log = logging.getLogger(__name__)


TOKEN_PATH = Path(os.environ["EON_TOKEN_PATH"])
SOURCE_CONFIG_PATH = Path(os.environ["EON_SOURCE_CONFIG_PATH"])



def load_meas(start_date: datetime, end_date: datetime):
    setup_console(os.getenv("LOG_LEVEL", "INFO"))
    
    with open(SOURCE_CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)

    query = build_query(start_date, end_date, config)

    ts = TokenStore(path=TOKEN_PATH)


    client=EonClient(query, ts)
    try:
        result= client.get_measurements()
    except Exception as e:
        log.exception("Failed to fetch measurements from EON API")
        raise

    if result.points:
        first = result.points[0]
        last = result.points[-1]
        log.info("First point: %s %s", fmt_ts_utc(first.timestamp), first.values)
        log.info("Last point:  %s %s", fmt_ts_utc(last.timestamp), last.values)

        write_series(result)
    else:
        log.warning("No datapoints returned for the requested range.")
        log.info("Total count: %d measurement points.", len(result.points))
    

def fmt_ts_utc(ts: int) -> str:
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")

def build_query(start_day, end_day, config):
    if start_day > end_day: 
        raise ValueError("Start_day must be <= end_day")
    
    query=EonQuery(
        pod=config["pod"],
        var_mappings=config["var_mappings"],
        interval=config["interval"]["code"],
        start_date=datetime.combine(start_day, time(0,0,1)),
        end_date=datetime.combine(end_day, time(23,59,59))
        )
    return query



