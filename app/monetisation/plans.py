from datetime import timedelta

PLANS = {
    "monthly": {
        "refresh_interval_days": 30,
        "max_uploads_per_period": 1,
        "requires_active_payment": True,
    },
    "weekly": {
        "refresh_interval_days": 7,
        "max_uploads_per_period": 1,
        "requires_active_payment": True,
    },
    "daily": {
        "refresh_interval_days": 1,
        "max_uploads_per_period": 1,
        "requires_active_payment": True,
    },
}
