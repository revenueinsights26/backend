from datetime import datetime, timedelta
from fastapi import HTTPException
from app.db.db import get_connection
from app.monetisation.plans import PLANS


def enforce_plan_limits(owner_id: str, plan: str):
    rules = PLANS.get(plan)
    if not rules:
        raise HTTPException(status_code=403, detail="Unknown subscription plan")

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT MAX(created_at)
        FROM snapshots s
        JOIN hotels h ON s.hotel_id = h.hotel_id
        WHERE h.owner_id = ?
        """,
        (owner_id,),
    )

    row = cur.fetchone()
    conn.close()

    # First upload ever → always allowed
    if not row or not row[0]:
        return

    last_upload = datetime.fromisoformat(row[0])
    allowed_after = last_upload + timedelta(days=rules["refresh_interval_days"])

    if datetime.utcnow() < allowed_after:
        raise HTTPException(
            status_code=403,
            detail=f"Plan limit reached. Next upload allowed after {allowed_after.isoformat()}",
        )
