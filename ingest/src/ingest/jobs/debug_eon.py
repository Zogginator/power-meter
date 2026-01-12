import os
import logging

from pathlib import Path
from datetime import datetime

from ingest.logging_setup import setup_console
from ingest.eon.client import EonClient, EonQuery, TokenStore, MeasurementSeries, MeasurementPoint
from ingest.influx.writer import write_series


log = logging.getLogger(__name__)

#token = "8Y98sGKHgokRhbb_A2T04zavhZdHm1iE"
TOKEN_PATH = Path(os.environ["EON_TOKEN_PATH"])


def main():
    setup_console(os.getenv("LOG_LEVEL", "INFO"))
    log.info("Debug EON ingest starts")

    query = EonQuery(
        pod="HU000210F11-E647651230609-4000001",
        measured_vars="+A,-A",
        interval=1,
        start_day=datetime(2026, 1, 6, 0, 0, 1),
        end_day=datetime(2026, 1, 6, 23, 59, 59),
        )
    
    ts = TokenStore(path=TOKEN_PATH)


    client=EonClient(query, ts)
    result= client.get_measurements()

    #log.info("First %s", result.points[0])
    log.info("Total count: %d measurement points.", len(result.points))
    write_series(result)


if __name__ == "__main__":
    main()


