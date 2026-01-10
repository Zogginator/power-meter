import os
import requests
import json
from dataclasses import dataclass 
from datetime import datetime
from urllib.parse import quote
from pathlib import Path
from typing import Optional

#token = "8Y98sGKHgokRhbb_A2T04zavhZdHm1iE"
#assert token, "EON_TOKEN env var not set"
TOKEN_PATH = Path.home() / "OneDrive" / "Documents" / "Projects" /"power-meter" / "data" / "token.json"
@dataclass
class TokenStore:
    path: Path

    def load(self) -> Optional[str]:
        if not self.path.exists():
            return None
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            tok = data.get("token")
            return tok if isinstance(tok, str) and tok else None
        except Exception:
            return None

    def save(self, token: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({"token": token}, ensure_ascii=False, indent=2), encoding="utf-8")

    def clear(self) -> None:
        if self.path.exists():
            self.path.unlink()

@dataclass (frozen=True)
class EonQuery:
    start_day: datetime     # only the day matters, time is discarded
    end_day: datetime       #same
    pod: str = "HU000210F11-E647651230609-4000001"
    measured_vars: str = "+A,-A" #full list: +A,-A,+Ri,-Rc,+R,-R
    interval: int = 1 #1: 15 minutes, 2: day 3: month
    language: str = "H"
    format: str = "json"

@dataclass
class SapODataError(Exception):
    http_status: int
    code: str
    message: str

def parse_sap_odata_error(resp) -> Optional[SapODataError]:
    ct = (resp.headers.get("content-type") or "").lower()
    if "application/json" not in ct:
        return None
    try:
        j = resp.json()
    except Exception:
        return None

    err = j.get("error")
    if not isinstance(err, dict):
        return None

    code = err.get("code") or ""
    msg = ""
    m = err.get("message")
    if isinstance(m, dict):
        msg = m.get("value") or ""
    elif isinstance(m, str):
        msg = m

    return SapODataError(resp.status_code, code, msg)

def build_eon_url(q: EonQuery):    
    base = "https://e-portal.eon-hungaria.com/sap/opu/odata/sap/ZWB5_W1000"

    key_part = (
        f"MeasData("
        f"Pod='{q.pod}',"
        f"MeasVarList='{q.measured_vars}',"
        f"Interval='{q.interval}',"
        f"StartDate=datetime'{q.start_day.isoformat()}',"
        f"EndDate=datetime'{q.end_day.isoformat()}'"
        f")"
    )

    return (
        f"{base}/{quote(key_part, safe='()=,:\'')}"
        f"?$expand=MeasDatas"
        f"&sap-language={q.language}"
        f"&$format={q.format}"
    )

query = EonQuery(
    pod="HU000210F11-E647651230609-4000001",
    measured_vars="+A,-A",
    interval=1,
    start_day=datetime(2026, 1, 6, 0, 0, 0),
    end_day=datetime(2026, 1, 6, 23, 59, 59),
)
url = build_eon_url(query)
print(url)

def build_headers(token: str) -> dict:
    return {
        "Accept": "application/json",
        "authorizationerp": f"Bearer {token}",
        "X-Requested-With": "X",
    }


def fetch_meas(url: str, token_store: TokenStore) -> dict:
    token = token_store.load()
    if not token:
        raise RuntimeError("No stored token. Refresh needed.")

    r = requests.get(url, headers=build_headers(token), timeout=30)
    if r.ok:
        return r.json()

    sap_err = parse_sap_odata_error(r)
    if sap_err and is_auth_error(sap_err):
        token_store.clear() # useless token in recycle bin.
        raise RuntimeError(f"Auth erroe ({sap_err.code}): {sap_err.message}. Token discarded, refresh/login needed.")

    # other error
    if sap_err:
        raise RuntimeError(f"SAP OData error {sap_err.http_status} ({sap_err.code}): {sap_err.message}")
    r.raise_for_status()
    raise RuntimeError("unknown SAP error.")

ts=TokenStore(path=TOKEN_PATH)

payload = fetch_meas(url, ts)


#with open("meas_one.json", "wb") as f:
#    f.write(r.content)
#print("saved: meas_one.json", "bytes:", len(r.content))

def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def normalize_row(row):
    return{
        "timestamp_sec": int(row["Timestamp"][6:-2]) // 1000, # ms > sec
        "pod": row["Pod"],
        "in_kwh": to_float(row.get("Num1")),
        "out_kwh": to_float(row.get("Num2"))
    }

norm_data=[normalize_row(r) for r in  payload["d"]["MeasDatas"]["results"]]

print(norm_data)
