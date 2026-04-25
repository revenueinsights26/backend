from fastapi import FastAPI, Body, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Any
import os
import uuid
import secrets
import json
from datetime import datetime, timedelta
import calendar
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool

# Optional AI (kept safe; if key missing, commentary becomes None)
try:
    from openai import OpenAI
    _openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
except Exception:
    _openai_client = None


# -------------------------------------------------
# App setup
# -------------------------------------------------

# Disable docs in production
if os.getenv("ENVIRONMENT") == "production":
    app = FastAPI(title="Revenue Insights & Pricing Console", version="2.0", docs_url=None, redoc_url=None, openapi_url=None)
else:
    app = FastAPI(title="Revenue Insights & Pricing Console", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection
DATABASE_URL = os.getenv("DATABASE_URL")

# Create connection pool
db_pool = SimpleConnectionPool(1, 10, DATABASE_URL)

def get_conn():
    return db_pool.getconn()

def put_conn(conn):
    db_pool.putconn(conn)

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    
    # Create owners table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS owners (
            owner_id TEXT PRIMARY KEY,
            owner_name TEXT NOT NULL,
            email TEXT NOT NULL,
            service_tier TEXT DEFAULT 'pro',
            is_active INTEGER DEFAULT 1,
            access_token TEXT UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create hotels table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hotels (
            hotel_id TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            hotel_name TEXT NOT NULL,
            rooms_available INTEGER DEFAULT 10,
            currency_code TEXT DEFAULT 'ZAR',
            currency_symbol TEXT DEFAULT 'R',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (owner_id) REFERENCES owners(owner_id)
        )
    """)
    
    # Create snapshots table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            snapshot_id TEXT PRIMARY KEY,
            hotel_id TEXT NOT NULL,
            period_start TEXT NOT NULL,
            period_end TEXT NOT NULL,
            occupancy REAL,
            adr REAL,
            revpar REAL,
            room_revenue REAL,
            forecast_occupancy REAL,
            forecast_adr_min REAL,
            forecast_adr_max REAL,
            commentary TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (hotel_id) REFERENCES hotels(hotel_id)
        )
    """)
    
    # Create daily_performance table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_performance (
            id SERIAL PRIMARY KEY,
            snapshot_id TEXT NOT NULL,
            hotel_id TEXT NOT NULL,
            stay_date TEXT NOT NULL,
            rooms_sold INTEGER,
            room_revenue REAL,
            adr REAL,
            FOREIGN KEY (snapshot_id) REFERENCES snapshots(snapshot_id)
        )
    """)
    
    # Create daily_compset table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_compset (
            id SERIAL PRIMARY KEY,
            snapshot_id TEXT NOT NULL,
            hotel_id TEXT NOT NULL,
            stay_date TEXT NOT NULL,
            your_rate REAL,
            comp_rates_json TEXT,
            FOREIGN KEY (snapshot_id) REFERENCES snapshots(snapshot_id)
        )
    """)
    
    conn.commit()
    cur.close()
    put_conn(conn)


@app.on_event("startup")
def startup():
    init_db()


# -------------------------------------------------
# Request models
# -------------------------------------------------

class PerfRow(BaseModel):
    date: str
    rooms_sold: int
    room_revenue: float

class CompRow(BaseModel):
    date: str
    your_rate: Optional[float] = None
    comps: List[Optional[float]] = []

class CalculateRequest(BaseModel):
    hotel_id: str
    period_start: str
    period_end: str
    rooms_available: int
    performance_data: List[PerfRow]
    compset_data: List[CompRow] = []
    period_type: str = "monthly"


# -------------------------------------------------
# Rate Intelligence Models
# -------------------------------------------------

class RateIntelRequest(BaseModel):
    current_rate: float
    competitor_rates: List[float] = []
    historical_occupancy: float
    dow_factor: float = 50
    overall_avg_occ: float = 50

class RateIntelResponse(BaseModel):
    suggested_rate: float
    confidence_score: int
    recommendation: str
    confidence_level: str


# -------------------------------------------------
# Auth helpers
# -------------------------------------------------

def get_owner_by_token(token: str):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        "SELECT owner_id, service_tier, is_active FROM owners WHERE access_token = %s",
        (token,),
    )
    row = cur.fetchone()
    cur.close()
    put_conn(conn)

    if not row:
        raise HTTPException(status_code=401, detail="Invalid owner token")

    if row["is_active"] == 0:
        raise HTTPException(status_code=403, detail="Subscription inactive")

    return row


def get_hotel_rooms_available(hotel_id: str) -> int:
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT rooms_available FROM hotels WHERE hotel_id = %s", (hotel_id,))
    row = cur.fetchone()
    cur.close()
    put_conn(conn)
    return row["rooms_available"] if row else 100


def verify_hotel_ownership(owner_id: str, hotel_id: str):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT 1 FROM hotels WHERE hotel_id = %s AND owner_id = %s", (hotel_id, owner_id))
    ok = cur.fetchone()
    cur.close()
    put_conn(conn)
    if not ok:
        raise HTTPException(status_code=403, detail="Hotel does not belong to owner")


# -------------------------------------------------
# Utility helpers
# -------------------------------------------------

def safe_dict_row(row: dict) -> dict:
    """Convert dict to JSON-safe dict."""
    out = {}
    for k, v in row.items():
        if isinstance(v, bytes):
            out[k] = v.decode("utf-8", errors="ignore")
        elif isinstance(v, datetime):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


def compute_snapshot_kpis(perf_df: pd.DataFrame, rooms_available: int) -> dict:
    days = perf_df["date"].nunique()
    total_rooms_sold = perf_df["rooms_sold"].sum()
    total_revenue = perf_df["room_revenue"].sum()

    occ = (total_rooms_sold / (rooms_available * days)) * 100 if days > 0 else 0
    adr = (total_revenue / total_rooms_sold) if total_rooms_sold > 0 else 0
    revpar = (total_revenue / (rooms_available * days)) if days > 0 else 0

    return {
        "occupancy": float(round(occ, 2)),
        "adr": float(round(adr, 2)),
        "revpar": float(round(revpar, 2)),
        "room_revenue": float(round(total_revenue, 2)),
    }


# =========================================================
# 4-BRANCH FORECAST - REPLACES simple_forecast
# =========================================================

def four_branch_forecast(hotel_id: str, target_month: str, rooms_available: int) -> dict:
    """
    4-branch forecast for any future month - works even with limited data
    Branch 1: Seasonal (has same month last year)
    Branch 2: Trend-based (90+ days of history)
    Branch 3: Moving average (30-90 days)
    Branch 4: Limited data fallback
    """
    
    # Get historical snapshots for this hotel
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT * FROM snapshots 
        WHERE hotel_id = %s 
        ORDER BY created_at DESC
    """, (hotel_id,))
    snapshots = cur.fetchall()
    cur.close()
    put_conn(conn)
    
    # No data at all - default forecast
    if not snapshots:
        return {
            "forecast_occupancy": 50.0,
            "forecast_adr_min": 1000.0,
            "forecast_adr_max": 1500.0,
            "forecast_revpar": 500.0,
            "confidence": 30,
            "method": "Default (No Data)"
        }
    
    latest = snapshots[0]
    occupancy = latest["occupancy"]
    adr = latest["adr"]
    historical_months = len(snapshots)
    
    # Parse target month
    target_year = int(target_month[:4])
    target_month_num = int(target_month[5:7])
    
    # Get same month from previous years
    same_month_historical = []
    for s in snapshots:
        if s["period_start"] and s["period_start"][:7] == target_month:
            same_month_historical.append(s)
    
    # =========================================================
    # BRANCH 1: Has historical data for same month last year
    # =========================================================
    if len(same_month_historical) >= 1:
        avg_occ = sum(s["occupancy"] for s in same_month_historical) / len(same_month_historical)
        avg_adr = sum(s["adr"] for s in same_month_historical) / len(same_month_historical)
        
        # Calculate YoY trend if 2+ years available
        if len(same_month_historical) >= 2:
            yoy_occ = (same_month_historical[0]["occupancy"] - same_month_historical[1]["occupancy"]) / max(1, same_month_historical[1]["occupancy"])
            yoy_adr = (same_month_historical[0]["adr"] - same_month_historical[1]["adr"]) / max(1, same_month_historical[1]["adr"])
            # Cap YoY impact
            yoy_occ = max(-0.15, min(0.2, yoy_occ))
            yoy_adr = max(-0.1, min(0.15, yoy_adr))
            forecast_occ = avg_occ * (1 + yoy_occ)
            forecast_adr = avg_adr * (1 + yoy_adr)
        else:
            forecast_occ = avg_occ
            forecast_adr = avg_adr
        
        confidence = 85
        method = "Seasonal (Branch 1)"
        
    # =========================================================
    # BRANCH 2: 90+ days of data (3+ months)
    # =========================================================
    elif historical_months >= 3:
        sorted_snapshots = sorted(snapshots, key=lambda x: x["created_at"])
        # Calculate monthly trend
        occ_trend = (sorted_snapshots[-1]["occupancy"] - sorted_snapshots[0]["occupancy"]) / max(1, len(sorted_snapshots))
        adr_trend = (sorted_snapshots[-1]["adr"] - sorted_snapshots[0]["adr"]) / max(1, len(sorted_snapshots))
        
        forecast_occ = occupancy + occ_trend
        forecast_adr = adr + adr_trend
        
        confidence = 75
        method = "Trend-based (Branch 2)"
        
    # =========================================================
    # BRANCH 3: 30-90 days of data (1-3 months)
    # =========================================================
    elif historical_months >= 1:
        # Use current with slight adjustment
        forecast_occ = occupancy
        forecast_adr = adr
        
        confidence = 60
        method = "Moving Average (Branch 3)"
        
    # =========================================================
    # BRANCH 4: Limited data fallback
    # =========================================================
    else:
        forecast_occ = occupancy if occupancy > 0 else 50
        forecast_adr = adr if adr > 0 else 1200
        confidence = 45
        method = "Limited Data (Branch 4)"
    
    # Ensure reasonable ranges
    forecast_occ = max(10, min(95, forecast_occ))
    forecast_adr = max(500, min(5000, forecast_adr))
    
    # Calculate RevPAR
    forecast_revpar = (forecast_occ / 100) * forecast_adr
    
    return {
        "forecast_occupancy": float(round(forecast_occ, 1)),
        "forecast_adr_min": float(round(forecast_adr * 0.9, 0)),
        "forecast_adr_max": float(round(forecast_adr * 1.1, 0)),
        "forecast_revpar": float(round(forecast_revpar, 0)),
        "confidence": confidence,
        "method": method
    }


def generate_commentary(kpis: dict, forecast: dict = None) -> Optional[str]:
    if _openai_client is None:
        return None

    forecast_text = ""
    if forecast:
        forecast_text = f"""
Forecast for next period:
- Expected Occupancy: {forecast['forecast_occupancy']}%
- ADR Range: {forecast['forecast_adr_min']} - {forecast['forecast_adr_max']}
- Method: {forecast.get('method', 'Standard')}
- Confidence: {forecast.get('confidence', 70)}%
"""

    prompt = f"""
You are a hotel revenue analyst.
Explain the performance factually and concisely.

Current Performance:
- Occupancy: {kpis['occupancy']}%
- ADR: {kpis['adr']}
- RevPAR: {kpis['revpar']}
- Room Revenue: {kpis['room_revenue']}
{forecast_text}
Structure:
1. Executive summary
2. Key driver
3. Forecast outlook
Keep it under 150 words.
"""
    try:
        resp = _openai_client.responses.create(
            model="gpt-4o-mini",
            input=prompt,
            temperature=0.2,
            max_output_tokens=250
        )
        return resp.output_text
    except Exception:
        return None


# -------------------------------------------------
# PROTECTED RATE INTELLIGENCE ENDPOINT
# -------------------------------------------------

@app.post("/api/rate-intelligence")
def rate_intelligence(
    req: RateIntelRequest,
    x_owner_token: str = Header(..., alias="X-Owner-Token"),
):
    owner = get_owner_by_token(x_owner_token)
    
    suggested = req.current_rate
    
    occ = req.historical_occupancy
    if occ >= 80:
        demand = 1.08
        demand_text = "high demand"
    elif occ >= 65:
        demand = 1.03
        demand_text = "good demand"
    elif occ >= 50:
        demand = 1.00
        demand_text = "moderate demand"
    elif occ >= 35:
        demand = 0.97
        demand_text = "soft demand"
    else:
        demand = 0.94
        demand_text = "low demand"
    
    suggested = suggested * demand
    
    comp_text = ""
    if req.competitor_rates and len(req.competitor_rates) > 0:
        comp_avg = sum(req.competitor_rates) / len(req.competitor_rates)
        if comp_avg > req.current_rate * 1.05:
            suggested = suggested * 1.03
            comp_text = "below competitors"
        elif comp_avg < req.current_rate * 0.95:
            suggested = suggested * 0.97
            comp_text = "above competitors"
        else:
            comp_text = "aligned with competitors"
    
    dow_adj = req.dow_factor / 50 if req.dow_factor > 0 else 1.0
    dow_adj = max(0.95, min(1.05, dow_adj))
    suggested = suggested * dow_adj
    
    suggested = round(suggested / 10) * 10
    
    comp_count = len(req.competitor_rates)
    if comp_count >= 5:
        confidence = 85
        level = "High"
    elif comp_count >= 3:
        confidence = 75
        level = "Medium"
    elif comp_count >= 1:
        confidence = 65
        level = "Medium"
    else:
        confidence = 50
        level = "Low"
    
    pct = ((suggested - req.current_rate) / req.current_rate) * 100
    
    if pct > 5:
        rec = f"Increase rate by {round(pct)}% - {demand_text}, {comp_text}"
    elif pct < -5:
        rec = f"Decrease rate by {abs(round(pct))}% - {demand_text}, {comp_text}"
    elif pct > 2:
        rec = f"Slight increase ({round(pct)}%) - {demand_text}"
    elif pct < -2:
        rec = f"Slight decrease - {comp_text}"
    else:
        rec = f"Maintain current rate - {demand_text}, {comp_text}"
    
    return RateIntelResponse(
        suggested_rate=suggested,
        confidence_score=confidence,
        recommendation=rec,
        confidence_level=level
    )


# -------------------------------------------------
# ADMIN: View All Clients
# -------------------------------------------------

@app.get("/admin/clients")
async def admin_clients():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    cur.execute("""
        SELECT owner_id, owner_name, email, service_tier, is_active, access_token, created_at 
        FROM owners 
        ORDER BY created_at DESC
    """)
    owners = [dict(row) for row in cur.fetchall()]
    
    cur.execute("""
        SELECT hotel_id, owner_id, hotel_name, rooms_available, currency_code, currency_symbol, created_at 
        FROM hotels 
        ORDER BY created_at DESC
    """)
    hotels = [dict(row) for row in cur.fetchall()]
    
    cur.close()
    put_conn(conn)
    
    return {
        "success": True,
        "total_owners": len(owners),
        "total_hotels": len(hotels),
        "owners": owners,
        "hotels": hotels
    }


# -------------------------------------------------
# Core endpoints
# -------------------------------------------------

@app.get("/")
def health():
    return {"status": "OK"}


@app.post("/owners/create")
def create_owner(
    owner_id: str = Body(...),
    owner_name: str = Body(...),
    email: str = Body(...),
    service_tier: str = Body(...),
):
    token = secrets.token_urlsafe(32)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO owners (owner_id, owner_name, email, service_tier, is_active, access_token)
        VALUES (%s, %s, %s, %s, 1, %s)
        """,
        (owner_id, owner_name, email, service_tier, token),
    )
    conn.commit()
    cur.close()
    put_conn(conn)
    return {"message": "Owner created", "owner_token": token}


@app.post("/hotels/create")
def create_hotel(
    hotel_id: str = Body(...),
    owner_id: str = Body(...),
    hotel_name: str = Body(...),
    rooms_available: int = Body(...),
    currency_code: str = Body(...),
    currency_symbol: str = Body(...),
):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO hotels (hotel_id, owner_id, hotel_name, rooms_available, currency_code, currency_symbol)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (hotel_id, owner_id, hotel_name, rooms_available, currency_code, currency_symbol),
    )
    conn.commit()
    cur.close()
    put_conn(conn)
    return {"message": "Hotel created"}


@app.post("/calculate_and_store")
def calculate_and_store(
    payload: CalculateRequest,
    x_owner_token: str = Header(..., alias="X-Owner-Token"),
):
    owner = get_owner_by_token(x_owner_token)
    verify_hotel_ownership(owner["owner_id"], payload.hotel_id)

    perf_df = pd.DataFrame([r.model_dump() for r in payload.performance_data])
    perf_df["rooms_sold"] = perf_df["rooms_sold"].fillna(0).astype(int)
    perf_df["room_revenue"] = perf_df["room_revenue"].fillna(0).astype(float)

    kpis = compute_snapshot_kpis(perf_df, payload.rooms_available)
    
    # USE 4-BRANCH FORECAST
    target_month = payload.period_start[:7] if payload.period_start else None
    if target_month:
        fc = four_branch_forecast(payload.hotel_id, target_month, payload.rooms_available)
    else:
        # Fallback
        fc = {
            "forecast_occupancy": kpis["occupancy"],
            "forecast_adr_min": kpis["adr"] * 0.97,
            "forecast_adr_max": kpis["adr"] * 1.03,
        }
    
    commentary = generate_commentary(kpis, fc)

    snapshot_id = str(uuid.uuid4())

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO snapshots (
          snapshot_id, hotel_id, period_start, period_end,
          occupancy, adr, revpar, room_revenue,
          forecast_occupancy, forecast_adr_min, forecast_adr_max,
          commentary
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            snapshot_id,
            payload.hotel_id,
            payload.period_start,
            payload.period_end,
            kpis["occupancy"],
            kpis["adr"],
            kpis["revpar"],
            kpis["room_revenue"],
            fc["forecast_occupancy"],
            fc["forecast_adr_min"],
            fc["forecast_adr_max"],
            commentary,
        ),
    )

    for _, r in perf_df.iterrows():
        rooms_sold = int(r["rooms_sold"])
        rev = float(r["room_revenue"])
        adr = rev / rooms_sold if rooms_sold > 0 else 0.0

        cur.execute(
            """
            INSERT INTO daily_performance (snapshot_id, hotel_id, stay_date, rooms_sold, room_revenue, adr)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (snapshot_id, payload.hotel_id, r["date"], rooms_sold, rev, float(round(adr, 2))),
        )

    for c in payload.compset_data:
        cur.execute(
            """
            INSERT INTO daily_compset (snapshot_id, hotel_id, stay_date, your_rate, comp_rates_json)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                snapshot_id,
                payload.hotel_id,
                c.date,
                c.your_rate,
                json.dumps(c.comps),
            ),
        )

    conn.commit()
    cur.close()
    put_conn(conn)

    return {"status": "stored", "snapshot_id": snapshot_id, "forecast": fc}


# =========================================================
# NEW: Forecast for any future month (Rate Intelligence)
# =========================================================

@app.post("/forecast_future_month")
def forecast_future_month(
    hotel_id: str = Body(...),
    target_month: str = Body(...),
    rooms_available: int = Body(...),
    x_owner_token: str = Header(..., alias="X-Owner-Token"),
):
    """Get 4-branch forecast for any future month - used by Rate Intelligence"""
    owner = get_owner_by_token(x_owner_token)
    verify_hotel_ownership(owner["owner_id"], hotel_id)
    
    forecast = four_branch_forecast(hotel_id, target_month, rooms_available)
    return forecast


@app.get("/hotel_dashboard/{hotel_id}")
def hotel_dashboard(
    hotel_id: str,
    x_owner_token: str = Header(..., alias="X-Owner-Token"),
):
    owner = get_owner_by_token(x_owner_token)
    verify_hotel_ownership(owner["owner_id"], hotel_id)

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT * FROM snapshots
        WHERE hotel_id = %s
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (hotel_id,),
    )
    row = cur.fetchone()
    cur.close()
    put_conn(conn)
    if not row:
        return {"message": "No data loaded"}
    return safe_dict_row(row)


@app.get("/hotel_dashboard_history/{hotel_id}")
def hotel_dashboard_history(
    hotel_id: str,
    x_owner_token: str = Header(..., alias="X-Owner-Token"),
):
    owner = get_owner_by_token(x_owner_token)
    verify_hotel_ownership(owner["owner_id"], hotel_id)

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT * FROM snapshots
        WHERE hotel_id = %s
        ORDER BY created_at ASC
        """,
        (hotel_id,),
    )
    rows = cur.fetchall()
    cur.close()
    put_conn(conn)
    return [safe_dict_row(r) for r in rows]


@app.get("/daily_by_snapshot/{snapshot_id}")
def daily_by_snapshot(
    snapshot_id: str,
    x_owner_token: str = Header(..., alias="X-Owner-Token"),
):
    owner = get_owner_by_token(x_owner_token)

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("SELECT hotel_id FROM snapshots WHERE snapshot_id = %s", (snapshot_id,))
    snap = cur.fetchone()
    if not snap:
        cur.close()
        put_conn(conn)
        raise HTTPException(status_code=404, detail="Snapshot not found")

    hotel_id = snap["hotel_id"]
    verify_hotel_ownership(owner["owner_id"], hotel_id)

    cur.execute(
        """
        SELECT stay_date, rooms_sold, room_revenue, adr
        FROM daily_performance
        WHERE snapshot_id = %s
        ORDER BY stay_date ASC
        """,
        (snapshot_id,),
    )
    perf_rows = [dict(row) for row in cur.fetchall()]

    cur.execute(
        """
        SELECT stay_date, your_rate, comp_rates_json
        FROM daily_compset
        WHERE snapshot_id = %s
        ORDER BY stay_date ASC
        """,
        (snapshot_id,),
    )
    comp_rows = []
    for r in cur.fetchall():
        comp_rows.append({
            "stay_date": r["stay_date"],
            "your_rate": r["your_rate"],
            "comps": json.loads(r["comp_rates_json"]) if r["comp_rates_json"] else []
        })

    cur.close()
    put_conn(conn)

    return {"hotel_id": hotel_id, "snapshot_id": snapshot_id, "performance": perf_rows, "compset": comp_rows}
