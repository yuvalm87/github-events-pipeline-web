"""DuckDB connection and initialization helpers."""
import logging
import os
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

DB_DIR = Path(__file__).parent.parent / "data" / "duckdb"
DB_PATH = DB_DIR / "github_events.duckdb"


def get_db_connection() -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection to the GitHub events database."""
    # Ensure database directory exists
    DB_DIR.mkdir(parents=True, exist_ok=True)
    
    # Connect to database
    conn = duckdb.connect(str(DB_PATH))
    return conn


def init_database() -> None:
    """Initialize the database schema if not already present."""
    conn = get_db_connection()
    try:
        # Read and execute schema SQL
        schema_path = Path(__file__).parent / "sql" / "schema.sql"
        with open(schema_path, "r") as f:
            schema_sql = f.read()
        
        conn.execute(schema_sql)
        logger.info(f"Database initialized at {DB_PATH}")
    finally:
        conn.close()


def get_db_path() -> str:
    """Get the absolute path to the DuckDB database file."""
    return str(DB_PATH)
