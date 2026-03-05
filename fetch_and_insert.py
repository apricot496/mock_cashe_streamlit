# scripts/fetch_and_insert.py
from __future__ import annotations

import json
import sqlite3
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


DB_PATH = Path("mock_cache_data.db")

# 東京・中央区（銀座付近の近似座標）
LAT = 35.6717
LON = 139.7650

OPEN_METEO_URL = (
    "https://api.open-meteo.com/v1/forecast"
    f"?latitude={LAT}&longitude={LON}"
    "&current=temperature_2m"
    "&timezone=Asia%2FTokyo"
)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS chuoh_temp (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    temp_c REAL NOT NULL
);
"""

CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_chuoh_temp_ts
ON chuoh_temp (ts);
"""

INSERT_SQL = "INSERT INTO chuoh_temp (ts, temp_c) VALUES (?, ?);"


def fetch_current_temp_c() -> tuple[str, float]:
    """Open-Meteoから現在気温(℃)と時刻を取得。tsはISO文字列で返す。"""
    with urllib.request.urlopen(OPEN_METEO_URL, timeout=20) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    current = payload.get("current") or {}
    temp_c = current.get("temperature_2m")
    ts_local = current.get("time")  # Asia/Tokyo の日時文字列（例: "2026-03-05T12:30"）

    if temp_c is None:
        raise RuntimeError(f"temperature_2m not found in response: {payload}")

    # DBには分かりやすくUTC ISOで保存（比較しやすい）
    # Open-Meteoのtimeはローカル文字列なので、ここでは「取得した瞬間のUTC」を使う
    ts_utc = datetime.now(timezone.utc).isoformat()

    return ts_utc, float(temp_c)


def ensure_db_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    cur.execute(CREATE_INDEX_SQL)
    conn.commit()


def insert_row(ts: str, temp_c: float) -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"{DB_PATH} not found. Create it first.")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        ensure_db_schema(conn)
        conn.execute(INSERT_SQL, (ts, temp_c))
        conn.commit()
    finally:
        conn.close()


def main() -> None:
    ts, temp_c = fetch_current_temp_c()
    insert_row(ts, temp_c)
    print(f"Inserted: ts={ts}, temp_c={temp_c:.1f}C into {DB_PATH}")


if __name__ == "__main__":
    main()
