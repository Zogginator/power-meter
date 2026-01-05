import os


def env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return v


INFLUX_URL = env("INFLUX_URL")
INFLUX_ORG = env("INFLUX_ORG")
INFLUX_BUCKET = env("INFLUX_BUCKET")
INFLUX_TOKEN = env("INFLUX_TOKEN")
