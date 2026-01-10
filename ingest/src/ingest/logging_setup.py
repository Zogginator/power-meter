import logging
import sys

def setup_console(level: str = "INFO") -> None:
    root = logging.getLogger()
    if root.handlers:  # no duplication
        return

    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
    
    root.addHandler(h)