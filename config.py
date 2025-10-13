import os

DB_URI = os.getenv(
    "F1_DB_URI",
    "postgresql+psycopg2://f1user:f1pass@localhost:5432/f1data"
)
FASTF1_CACHE = os.getenv("FASTF1_CACHE", "fastf1_cache")
