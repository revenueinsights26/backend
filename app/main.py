from fastapi import FastAPI, HTTPException, Depends, Security
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime
import os
import sqlite3
import json

# ─────────────────────────────────────────────
# FastAPI App Configuration
# ─────────────────────────────────────────────
# Disable docs in production
if os.getenv("ENVIRONMENT") == "production":
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
else:
    app = FastAPI()

# CORS - Only allow your frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://revenueinsights26.github.io"],
    allow_methods=["GET", "POST"],
    allow_headers=["X-Owner-Token", "Content-Type"],
)

# API Key security
API_KEY_HEADER = APIKeyHeader(name="X-Owner-Token")

# Database path
DB_PATH = os.path.join(os.path.dirname(__file__), "db", "revenue_insights.db")

# ─────────────────────────────────────────────
# Helper Functions
# ─────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

async def verify_token(api_key: str = Security(API_KEY_HEADER)):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT owner_id, is_active FROM owners WHERE access_token = ?", (api_key,))
    owner = cursor.fetchone()
    conn.close()
    
    if not owner or not owner["is_active"]:
        raise HTTPException(status_code=403, detail="Invalid or inactive token")
    return api_key

# ─────────────────────────────────────────────
# Request/Response Models
# ─────────────────────────────────────────────
class RateIntelligenceRequest(BaseModel):
    current_rate: float
    competitor_rates: List[float]
    historical_occupancy: float
    dow_factor: float
    overall_avg_occ: float
    has_competitor_data: bool = True

class RateIntelligenceResponse(BaseModel):
    suggested_rate: float
    confidence_score: int
    recommendation: str
    confidence_level: str

# ─────────────────────────────────────────────
# MATHEMATICAL SUGGESTED RATE FORMULA (HIDDEN)
# ─────────────────────────────────────────────
def calculate_demand_score(historical_occ: float) -> float:
    """Demand Score: 0.7 to 1.35 based on historical occupancy"""
    if historical_occ >= 80:
        return min(1.35, 1.15 + ((historical_occ - 80) / 100))
    elif historical_occ >= 65:
        return 1.0 + ((historical_occ - 65) / 100)
    elif historical_occ >= 50:
        return 0.85 + ((historical_occ - 50) / 100)
    else:
        return max(0.7, 0.7 + (historical_occ / 100))

def calculate_competitor_score(current_rate: float, competitor_rates: List[float]) -> float:
    """Competitor Score: 0.85 to 1.15 based on competitor positioning"""
    if not competitor_rates or len(competitor_rates) == 0:
        return 1.0
    
    comp_avg = sum(competitor_rates) / len(competitor_rates)
    comp_score = comp_avg / current_rate
    return max(0.85, min(1.15, comp_score))

def calculate_dow_score(dow_factor: float, overall_avg_occ: float) -> float:
    """DOW Score: 0.85 to 1.15 based on day-of-week patterns"""
    if overall_avg_occ <= 0:
        return 1.0
    dow_score = 1 + ((dow_factor - overall_avg_occ) / overall_avg_occ)
    return max(0.85, min(1.15, dow_score))

def calculate_confidence_score(has_comp_data: bool, comp_count: int, historical_data_points: int, has_dow_data: bool) -> int:
    """Confidence Score: 0-100% based on data quality"""
    confidence = 50
    
    if has_comp_data and comp_count >= 5:
        confidence += 30
    elif has_comp_data and comp_count >= 3:
        confidence += 20
    elif has_comp_data and comp_count >= 1:
        confidence += 10
    
    if historical_data_points >= 30:
        confidence += 20
    elif historical_data_points >= 15:
        confidence += 15
    elif historical_data_points >= 7:
        confidence += 10
    elif historical_data_points >= 3:
        confidence += 5
    
    if has_dow_data:
        confidence += 10
    
    return min(95, max(30, confidence))

def get_confidence_level(score: int) -> str:
    if score >= 80:
        return "High"
    elif score >= 60:
        return "Medium"
    elif score >= 30:
        return "Low"
    else:
        return "Very Low"

def generate_recommendation(current_rate: float, suggested_rate: float, confidence: int) -> str:
    """Generate human-readable recommendation"""
    percent_diff = ((suggested_rate - current_rate) / current_rate) * 100
    
    if confidence < 40:
        return "Limited data available. Use as general guide only."
    elif percent_diff > 5:
        return f"AI suggests +{round(percent_diff)}% increase based on strong demand and competitor positioning"
    elif percent_diff < -5:
        return f"AI suggests {round(percent_diff)}% decrease to stay competitive on soft demand days"
    elif percent_diff > 2:
        return f"AI suggests slight increase ({round(percent_diff)}%) - good demand expected"
    elif percent_diff < -2:
        return "AI suggests slight decrease to improve competitiveness"
    else:
        return "AI suggests maintaining current rate - balanced market position"

# ─────────────────────────────────────────────
# NEW: Rate Intelligence Endpoint (PROTECTED)
# ─────────────────────────────────────────────
@app.post("/api/rate-intelligence", response_model=RateIntelligenceResponse)
async def get_rate_intelligence(
    request: RateIntelligenceRequest,
    token: str = Depends(verify_token)
):
    """
    Protected endpoint that calculates suggested rates.
    Formula is hidden on the server.
    """
    
    # Step 1: Calculate scores
    demand_score = calculate_demand_score(request.historical_occupancy)
    comp_score = calculate_competitor_score(request.current_rate, request.competitor_rates)
    dow_score = calculate_dow_score(request.dow_factor, request.overall_avg_occ)
    
    # Step 2: Calculate suggested rate
    suggested_rate = request.current_rate * demand_score * comp_score * dow_score
    
    # Step 3: Apply bounds (±15%)
    suggested_rate = max(request.current_rate * 0.85, min(request.current_rate * 1.15, suggested_rate))
    suggested_rate = round(suggested_rate / 10) * 10  # Round to nearest 10
    
    # Step 4: Calculate confidence score
    comp_count = len(request.competitor_rates) if request.competitor_rates else 0
    confidence_score = calculate_confidence_score(
        request.has_competitor_data,
        comp_count,
        30,
        True
    )
    
    # Step 5: Generate recommendation
    recommendation = generate_recommendation(request.current_rate, suggested_rate, confidence_score)
    confidence_level = get_confidence_level(confidence_score)
    
    return RateIntelligenceResponse(
        suggested_rate=suggested_rate,
        confidence_score=confidence_score,
        recommendation=recommendation,
        confidence_level=confidence_level
    )

# ─────────────────────────────────────────────
# EXISTING ENDPOINTS (Keep your current ones)
# ─────────────────────────────────────────────

@app.get("/hotel_dashboard_history/{hotel_id}")
async def get_hotel_dashboard_history(
    hotel_id: str,
    token: str = Depends(verify_token)
):
    """Get all snapshots for a hotel"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM snapshots 
        WHERE hotel_id = ? 
        ORDER BY period_start DESC
    """, (hotel_id,))
    snapshots = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return snapshots

@app.get("/daily_by_snapshot/{snapshot_id}")
async def get_daily_by_snapshot(
    snapshot_id: int,
    token: str = Depends(verify_token)
):
    """Get daily performance and compset data for a snapshot"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM daily_performance WHERE snapshot_id = ?", (snapshot_id,))
    performance = [dict(row) for row in cursor.fetchall()]
    
    cursor.execute("SELECT * FROM daily_compset WHERE snapshot_id = ?", (snapshot_id,))
    compset = [dict(row) for row in cursor.fetchall()]
    
    conn.close()
    return {"performance": performance, "compset": compset}

# Add your other existing endpoints here (calculate_and_store, etc.)
