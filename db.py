# src/db.py
import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine

# --- Ensure project root is in sys.path for standalone runs ---
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# --- Import DB_URI from config ---
try:
    from src.config import DB_URI
except ImportError:
    # fallback if running standalone
    import importlib.util
    cfg_path = project_root / "src" / "config.py"
    spec = importlib.util.spec_from_file_location("config", str(cfg_path))
    cfg = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cfg)
    DB_URI = cfg.DB_URI

# --- Create SQLAlchemy engine ---
engine = create_engine(DB_URI)

def push_to_db(df: pd.DataFrame, table_name: str):
    """
    Push a DataFrame to the database table.
    Automatically skips if DataFrame is empty.
    """
    if df.empty:
        print(f"No data to insert into {table_name}")
        return
    df.to_sql(table_name, engine, if_exists="append", index=False)
    print(f"Inserted {len(df)} rows into {table_name}")

# --- Optional test when running this file directly ---
from sqlalchemy import text

if __name__ == "__main__":
    print("Testing DB connection...")
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("✅ Connection successful:", result.scalar())
    except Exception as e:
        print("❌ Connection failed:", e)
