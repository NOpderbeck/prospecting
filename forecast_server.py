"""
forecast_server.py — Standalone FastAPI server for the Forecast Dashboard.

Designed for Cloud Run. Reads forecast data from GCS (if GCS_BUCKET is set)
or falls back to the local reports/ directory.

Usage:
    python3 forecast_server.py
"""

import os
import json
import asyncio
import subprocess
import sys
import time as _time
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse

GCS_BUCKET = os.getenv("GCS_BUCKET", "")
BASE_DIR = Path(__file__).parent

app = FastAPI(title="Forecast Dashboard")

# ---------------------------------------------------------------------------
# Cached Salesforce connection — avoids SERVER_UNAVAILABLE from rapid logins
# ---------------------------------------------------------------------------
_sf_client = None
_sf_lock = __import__("threading").Lock()

def _get_sf():
    global _sf_client
    from simple_salesforce import Salesforce as _SF
    with _sf_lock:
        if _sf_client is None:
            _sf_client = _SF(
                username=os.environ["SF_USERNAME"],
                password=os.environ["SF_PASSWORD"],
                security_token=os.environ["SF_SECURITY_TOKEN"],
            )
    return _sf_client

def _sf_query(soql: str):
    global _sf_client
    from simple_salesforce import SalesforceExpiredSession
    try:
        return _get_sf().query_all(soql)
    except SalesforceExpiredSession:
        with _sf_lock:
            _sf_client = None
        return _get_sf().query_all(soql)

def _sf_query_one(soql: str):
    global _sf_client
    from simple_salesforce import SalesforceExpiredSession
    try:
        return _get_sf().query(soql)
    except SalesforceExpiredSession:
        with _sf_lock:
            _sf_client = None
        return _get_sf().query(soql)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _gcs_client():
    from google.cloud import storage as _gcs
    return _gcs.Client()


def _get_gcs_text(blob_name: str) -> str:
    """Download a text blob from GCS."""
    bucket = _gcs_client().bucket(GCS_BUCKET)
    return bucket.blob(blob_name).download_as_text()


def _get_forecast_data() -> dict:
    """Load forecast_data.json from GCS (if configured) or local disk."""
    if GCS_BUCKET:
        try:
            return json.loads(_get_gcs_text("forecast_data.json"))
        except Exception as e:
            raise HTTPException(
                status_code=503,
                detail=f"Could not load forecast data from GCS: {e}",
            )
    else:
        local_path = BASE_DIR / "reports" / "forecast_data.json"
        if not local_path.exists():
            raise HTTPException(
                status_code=503,
                detail="forecast_data.json not found locally and GCS_BUCKET is not set.",
            )
        try:
            return json.loads(local_path.read_text())
        except Exception as e:
            raise HTTPException(
                status_code=503,
                detail=f"Could not read local forecast_data.json: {e}",
            )


def _get_dashboard_html() -> str:
    """Load forecast_review.html from GCS (if configured) or local disk.
    Storing the HTML in GCS means UI updates deploy without a Docker rebuild."""
    if GCS_BUCKET:
        try:
            return _get_gcs_text("forecast_review.html")
        except Exception as e:
            raise HTTPException(
                status_code=503,
                detail=f"Could not load dashboard HTML from GCS: {e}",
            )
    else:
        local_path = BASE_DIR / "reports" / "forecast_review.html"
        if not local_path.exists():
            raise HTTPException(status_code=404, detail="forecast_review.html not found")
        return local_path.read_text()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(content=_get_dashboard_html())


@app.get("/forecast_data.json")
async def forecast_data():
    data = _get_forecast_data()
    return JSONResponse(content=data, headers={"Cache-Control": "no-store"})


@app.get("/api/opp/{opp_id}")
async def get_opp_detail(opp_id: str):
    """Return volume / usage estimate fields for a single Opportunity."""
    try:
        fields = (
            "Id, Name, StageName, Amount, CloseDate, Owner.Name, "
            "Primary_API_Endpoint__c, "
            "Vol_Estimate_API_Calls_After_90_Day__c, "
            "Volume_Estimate_API_Monthly_Calls__c, "
            "Total_Potential_Volume__c, "
            "Estimated_Annualized_Revenue_Potential__c, "
            "Blended_Estimated_Cost_API_Call_CPM__c"
        )
        result = _sf_query_one(f"SELECT {fields} FROM Opportunity WHERE Id = '{opp_id}' LIMIT 1")
        records = result.get("records", [])
        if not records:
            raise HTTPException(status_code=404, detail="Opportunity not found")
        r = records[0]
        return {
            "opp_id":               r["Id"],
            "opp_name":             r["Name"],
            "stage":                r["StageName"],
            "amount":               r["Amount"],
            "close_date":           r["CloseDate"],
            "owner":                (r.get("Owner") or {}).get("Name"),
            "primary_endpoint":     r.get("Primary_API_Endpoint__c"),
            "api_calls_month3":     r.get("Vol_Estimate_API_Calls_After_90_Day__c"),
            "api_calls_steady":     r.get("Volume_Estimate_API_Monthly_Calls__c"),
            "volume_upside":        r.get("Total_Potential_Volume__c"),
            "annual_rev_potential": r.get("Estimated_Annualized_Revenue_Potential__c"),
            "blended_cpm":          r.get("Blended_Estimated_Cost_API_Call_CPM__c"),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/pago-usage")
async def get_pago_usage(ids: str = ""):
    """Return 30d API call totals per account for PayGo health table.
    ids: comma-separated Salesforce Account IDs"""
    account_ids = [i.strip() for i in ids.split(",") if i.strip()]
    if not account_ids:
        return {}
    try:
        ids_str = "', '".join(account_ids)
        result = _sf_query(f"""
            SELECT Account__c, API_Calls_Last_30_Days__c
            FROM Product_User__c
            WHERE Account__c IN ('{ids_str}')
        """)
        totals: dict[str, float] = {}
        for r in result.get("records", []):
            acct_id = r.get("Account__c") or ""
            calls   = r.get("API_Calls_Last_30_Days__c") or 0
            if acct_id:
                totals[acct_id] = totals.get(acct_id, 0) + calls
        return totals
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/refresh-forecast")
async def refresh_forecast():
    """Trigger the forecast-refresh Cloud Run Job to pull fresh data into GCS."""
    try:
        import google.auth
        import google.auth.transport.requests
        import requests as _req

        creds, project = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        auth_req = google.auth.transport.requests.Request()
        creds.refresh(auth_req)

        region  = os.getenv("CLOUD_RUN_REGION", "us-central1")
        job     = os.getenv("REFRESH_JOB_NAME", "forecast-refresh")
        url     = (
            f"https://{region}-run.googleapis.com/apis/run.googleapis.com/v1"
            f"/namespaces/{project}/jobs/{job}:run"
        )
        resp = _req.post(url, headers={"Authorization": f"Bearer {creds.token}"}, timeout=15)
        if not resp.ok:
            raise HTTPException(status_code=502, detail=f"Job trigger failed: {resp.text}")

        return {"ok": True, "message": "Refresh job triggered — data will update in ~2 min"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/healthz")
async def healthz():
    return {"ok": True}


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("forecast_server:app", host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
