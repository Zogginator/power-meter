import os
import logging
import sys
from pathlib import Path

def setup_logging(level: str = "INFO") -> None:

    level =  os.getenv("LOG_LEVEL", level).upper()
    log_dir = Path(os.getenv("LOG_DIR", "logs"))
    log_dir.mkdir(exist_ok=True)

    root = logging.getLogger()
    root.handlers.clear()

    root.setLevel(getattr(logging, level, logging.INFO))
    
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter(
        "%(levelname)s %(name)s: %(message)s"
    ))

    file = logging.FileHandler(log_dir / "daily.log", encoding="utf-8")
    file.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s"
    ))
    
    root.addHandler(console)
    root.addHandler(file)