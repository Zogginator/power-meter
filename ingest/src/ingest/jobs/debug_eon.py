import os
import logging
import json

from pathlib import Path
from datetime import datetime, time

from ingest.logging_setup import setup_console
from ingest.eon.client import EonClient, EonQuery, TokenStore, MeasurementSeries, MeasurementPoint
from ingest.influx.writer import write_series


log = logging.getLogger(__name__)

#token = "8Y98sGKHgokRhbb_A2T04zavhZdHm1iE"
TOKEN_PATH = Path(os.environ["EON_TOKEN_PATH"])
SOURCE_CONFIG_PATH = Path(os.environ["EON_SOURCE_CONFIG_PATH"])

def main():
    setup_console(os.getenv("LOG_LEVEL", "INFO"))
    log.info("Debug EON ingest starts")
    
    with open(SOURCE_CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)


    query = build_query(datetime(2026,1,10), datetime(2026,1,10), config)

    ts = TokenStore(path=TOKEN_PATH)


    client=EonClient(query, ts)
    result= client.get_measurements()

    log.info("First %s", result.points[0])
    log.info("Total count: %d measurement points.", len(result.points))
    write_series(result)


def build_query(start_day, end_day, config):
    if start_day > end_day: 
        raise ValueError("start_day must be <= end_day")
    
    query=EonQuery(
        pod=config["pod"],
        var_mappings=config["var_mappings"],
        interval=config["interval"]["code"],
        start_date=datetime.combine(start_day, time(0,0,1)),
        end_date=datetime.combine(end_day, time(23,59,59))
        )
    return query




if __name__ == "__main__":
    main()


