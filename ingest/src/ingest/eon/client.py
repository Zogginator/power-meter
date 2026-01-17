# client to the EON API
# TODOs:
#   - exception handlin esp. SAP
#   - Token manangement: re-request access token if auth fails
#   - mapping of the MeasuremenPoint fields with the fields Eon provides. No it is hardwired eg. in_kwh : Num1
#   - loging 
#   - build in date interval validation (30 days, ) 

from __future__ import annotations


import requests, json, logging

from dataclasses import dataclass
from datetime import datetime
from urllib.parse import quote
from pathlib import Path



log = logging.getLogger(__name__)


@dataclass (frozen=True)
class EonQuery:
    # date range can should be 
    # - max. 30 days in case of 15 minutes data (interval=1)
    # - max. 180 days in case of daily data (interval=2)
    # - max. 365 days in cas of monthly data (interval=3)
    # it seems that data is data is valid until current day 
    start_date: datetime     # only the day matters, time should be discarded in EONs own query T00:00:01
    end_date: datetime       # same T23.59.59
    pod: str                 # power meter id
    var_mappings: list      # "+A,-A" normally, full list: +A,-A,+Ri,-Rc,+R,-R
    interval: int = 1 #1: 15 minutes, 2: day 3: month
    language: str = "H"   
    format: str = "json"

@dataclass(frozen=True)
class MeasurementSeries:        # The response data structure 
    pod_id: str
    interval: str
    source: str
    points: list[MeasurementPoint]

@dataclass(frozen=True)
class MeasurementPoint:
    timestamp: datetime
    values: dict[str, float]

@dataclass
class TokenStore:       # to be refined
    path: Path

    def load(self) -> Optional[str]:
        
        if not self.path.exists():
            return None
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            tok = data.get("token")
            return tok if isinstance(tok, str) and tok else None
        except Exception:                ##more meaningful exception needed!"Token path exists, but token load failed"

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
        measured_vars=",".join(v["measured_var"] for v in q.var_mappings)   #normally: "+A, -A"
        log.info("Measured vars: %s", measured_vars)
    
        key_part = (
            f"MeasData("
            f"Pod='{q.pod}',"
            f"MeasVarList='{measured_vars}',"   # full list: +A,-A,+Ri,-Rc,+R,-R
            f"Interval='{q.interval}',"
            f"StartDate=datetime'{q.start_date.isoformat()}',"
            f"EndDate=datetime'{q.end_date.isoformat()}'"
            f")"
        )
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
    def _fetch_meas(self) -> dict: 
        log.info("Fetch form Eon API started")          # This function calls the API
        token = self.token_store.load()                 # get the stored token

        if not token:
            raise RuntimeError("No stored token. Refresh needed.")
        
        url = self._build_eon_url()    
        headers = self._build_headers(token)
        
        r = requests.get(url=url, headers=headers, timeout=30)
        if r.ok:
            log.info("Succesful fetch from EON")
            return r.json()

        sap_err = parse_sap_odata_error(r)
        if sap_err and is_auth_error(sap_err):
            token_store.clear() # useless token in recycle bin.
            raise RuntimeError(f"Auth error ({sap_err.code}): {sap_err.message}. Token not valid, refresh/login needed.")

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
        values={}
        for vm in self.query.var_mappings:
            db_field=vm["db_field"]
            response_field=vm["response_field"]
            raw=row.get(response_field)
            val= self._to_float(raw)

            if val is not None:
                values[db_field] = val            #values = {"in_kwh": 0,001,"out_kwh":0.002}

        return MeasurementPoint(
            timestamp = int(row["Timestamp"][6:-2]) // 1000, # ms > sec
            values = values
        )

    def get_measurements(self) -> MeasurementSeries:            ### This is the main function
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
        

    





