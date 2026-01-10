# client to the EON API
# 
import requests, json

from dataclasses import dataclass
from datetime import datetime
from urllib.parse import quote
from pathlib import Path
from typing import Mapping

token = "8Y98sGKHgokRhbb_A2T04zavhZdHm1iE"
assert token, "EON_TOKEN env var not set"
TOKEN_PATH = Path.home() / "OneDrive" / "Documents" / "Projects" /"power-meter" /".power_meter" / "eon" / "token.json"


@dataclass (frozen=True)
class EonQuery:
    start_day: datetime     # only the day matters, time is discarded
    end_day: datetime       #same
    pod: str = "HU000210F11-E647651230609-4000001" # hard wired for the time being
    measured_vars: str = "+A,-A" #full list: +A,-A,+Ri,-Rc,+R,-R
    interval: int = 1 #1: 15 minutes, 2: day 3: month
    language: str = "H"   
    format: str = "json"

@dataclass(frozen=True)
class MeasurementSeries:
    pod_id: str
    interval: str
    source: str
    points: list[MeasurementPoint]

@dataclass(frozen=True)
class MeasurementPoint:
    timestamp: datetime
    values: dict[str, float]

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
        except Exception:                                               ##more meaningful exception needed!"Token path exists, but token load failed"

            return None

    def save(self, token: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({"token": token}, ensure_ascii=False, indent=2), encoding="utf-8")

    def clear(self) -> None:
        if self.path.exists():
            pass
            # self.path.unlink()            ## not yet

class EonClient:
    def __init__(self, query: EonQuery, token_store: TokenStore):
        self.query = query
        self.token_store = token_store

    def _build_eon_url(self):    
        base = "https://e-portal.eon-hungaria.com/sap/opu/odata/sap/ZWB5_W1000"
        q=self.query
        key_part = (
            f"MeasData("
            f"Pod='{q.pod}',"
            f"MeasVarList='{q.measured_vars}',"
            f"Interval='{q.interval}',"
            f"StartDate=datetime'{q.start_day.isoformat()}',"
            f"EndDate=datetime'{q.end_day.isoformat()}'"
            f")"
        )
        print( f"EndDate=datetime'{q.end_day.isoformat()}',")
        return (
            f"{base}/{quote(key_part, safe='()=,:\'')}"
            f"?$expand=MeasDatas"
            f"&sap-language={q.language}"
            f"&$format={q.format}"
        )
    def _build_headers(self, token: str) -> dict:
        return {
            "Accept": "application/json",
            "authorizationerp": f"Bearer {token}",
            "X-Requested-With": "X",
        }
    def _fetch_meas(self) -> dict:                                  # This function calls the API
        token = self.token_store.load()

        if not token:
            raise RuntimeError("No stored token. Refresh needed.")
        
        url = self._build_eon_url()    
        headers = self._build_headers(token)
        
        r = requests.get(url=url, headers=headers, timeout=30)
        if r.ok:
            return r.json()

        sap_err = parse_sap_odata_error(r)
        if sap_err and is_auth_error(sap_err):
            token_store.clear() # useless token in recycle bin.
            raise RuntimeError(f"Auth error ({sap_err.code}): {sap_err.message}. Token discarded, refresh/login needed.")

        # other error
        if sap_err:
            raise RuntimeError(f"SAP OData error {sap_err.http_status} ({sap_err.code}): {sap_err.message}")
        r.raise_for_status()
        raise RuntimeError("unknown SAP error.")
    
    def _to_float(self, value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _normalize_meas(self, row):
        return MeasurementPoint(
            timestamp = int(row["Timestamp"][6:-2]) // 1000, # ms > sec
            values = {
                "in_kwh":  self._to_float(row.get("Num1")),
                "out_kwh": self._to_float(row.get("Num2"))}
        )

    def get_measurements(self) -> MeasurementSeries:                    ### This is the main function
        payload = self._fetch_meas()
        points = []

        for r in payload["d"]["MeasDatas"]["results"]:
            point = self._normalize_meas(r)
            points.append(point)

        return MeasurementSeries(
            pod_id = self.query.pod,
            interval = self.query.interval,
            source = 'API',
            points = points
        )
        

    


query = EonQuery(
    pod="HU000210F11-E647651230609-4000001",
    measured_vars="+A,-A",
    interval=1,
    start_day=datetime(2026, 1, 6, 0, 1, 0),
    end_day=datetime(2026, 1, 6, 23, 59, 0),
)

ts = TokenStore(path=TOKEN_PATH)


client=EonClient(query, ts)
result= client.get_measurements()

print(result.points[0])


