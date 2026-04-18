from db import db
from pathlib import Path

SCHEMA_PATH = Path(__file__).with_name("schema.sql")

def init_db():
    conn = db.get_connection()
    cursor = conn.cursor()

    with open(SCHEMA_PATH, "r") as f:
        cursor.executescript(f.read())

    conn.commit()
    conn.close()
    print("✅ Database initialized successfully")


if __name__ == "__main__":
    init_db()
