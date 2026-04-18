-- =========================================================
-- Revenue Insights & Pricing Console - SQLite Schema
-- =========================================================

-- Owners
CREATE TABLE IF NOT EXISTS owners (
  owner_id TEXT PRIMARY KEY,
  owner_name TEXT NOT NULL,
  email TEXT NOT NULL,
  service_tier TEXT NOT NULL,
  is_active INTEGER DEFAULT 0,
  access_token TEXT
);

-- Hotels
CREATE TABLE IF NOT EXISTS hotels (
  hotel_id TEXT PRIMARY KEY,
  owner_id TEXT NOT NULL,
  hotel_name TEXT NOT NULL,
  rooms_available INTEGER NOT NULL,
  currency_code TEXT NOT NULL,
  currency_symbol TEXT NOT NULL
);

-- Snapshots (one per upload/refresh)
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
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily performance rows (hotel operational daily truth)
CREATE TABLE IF NOT EXISTS daily_performance (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  snapshot_id TEXT NOT NULL,
  hotel_id TEXT NOT NULL,
  stay_date TEXT NOT NULL,          -- YYYY-MM-DD
  rooms_sold INTEGER NOT NULL,
  room_revenue REAL NOT NULL,
  adr REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_daily_perf_hotel_date
ON daily_performance (hotel_id, stay_date);

CREATE INDEX IF NOT EXISTS idx_daily_perf_snapshot
ON daily_performance (snapshot_id);

-- Daily compset rows (rate shop / competitor daily truth)
CREATE TABLE IF NOT EXISTS daily_compset (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  snapshot_id TEXT NOT NULL,
  hotel_id TEXT NOT NULL,
  stay_date TEXT NOT NULL,          -- YYYY-MM-DD
  your_rate REAL,
  comp_rates_json TEXT              -- JSON string: [rate1, rate2, ...]
);

CREATE INDEX IF NOT EXISTS idx_daily_comp_snapshot
ON daily_compset (snapshot_id);

CREATE INDEX IF NOT EXISTS idx_daily_comp_hotel_date
ON daily_compset (hotel_id, stay_date);
