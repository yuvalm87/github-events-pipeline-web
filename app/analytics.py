"""Analytics queries for GitHub events data."""
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

import duckdb

from app.db import get_db_connection

logger = logging.getLogger(__name__)

# Path to analytics SQL queries
SQL_DIR = Path(__file__).parent / "sql"


def get_top_repos(days: int = 30, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Get top repositories by total events within a time window.
    
    Filters events by created_at timestamp and returns repositories
    ranked by total event count (descending).
    
    Args:
        days: Number of days back to include (default 30).
              Filters events where created_at > NOW() - INTERVAL days.
        limit: Maximum number of repositories to return (default 10).
    
    Returns:
        List of dictionaries, each with keys:
          - repo_name (str)
          - total_events (int)
          - unique_users (int)
          - push_events (int)
          - first_event_at (datetime)
          - last_event_at (datetime)
          - processed_at (datetime)
    
    Raises:
        Exception: If query execution fails.
    """
    conn = get_db_connection()
    try:
        # Load the SQL query
        sql_path = SQL_DIR / "top_repos.sql"
        with open(sql_path, "r") as f:
            query = f.read()
        
        # Compute the minimum timestamp for filtering (timezone-aware UTC)
        min_timestamp = datetime.now(timezone.utc) - timedelta(days=days)
        
        # Execute with parameterization: min_timestamp and limit
        result = conn.execute(
            query,
            [min_timestamp, limit]
        ).fetchall()
        
        # Convert results to list of dictionaries for consistency
        # DuckDB returns tuples; map to column names
        if not result:
            return []
        
        # Get column names from the query result description
        columns = [desc[0] for desc in conn.description]
        rows = [dict(zip(columns, row)) for row in result]
        
        return rows
    
    except Exception as e:
        logger.error(f"Error querying top repositories: {e}")
        raise
    finally:
        conn.close()

def get_user_sessions(days: int = 7, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Get user sessions based on activity patterns.
    
    A session is defined as a sequence of events for a user with no more
    than 30 minutes of inactivity between consecutive events. Sessions are
    ordered by start time (most recent first) for each user.
    
    The query uses an expanded time window to correctly capture sessions
    that started slightly before the requested window but continued into it.
    Only sessions ending at or after the window start are included in results.
    
    Args:
        days: Number of days back to include (default 7).
              Sessions must have session_end_at >= NOW() - INTERVAL days.
        limit: Maximum number of sessions to return (default 50).
    
    Returns:
        List of dictionaries, each with keys:
          - actor_login (str): GitHub user login
          - session_id (int): Deterministic session number per actor
          - session_start_at (datetime): Timestamp of first event in session
          - session_end_at (datetime): Timestamp of last event in session
          - events_in_session (int): Count of events in session
    
    Raises:
        Exception: If query execution fails.
    """
    conn = get_db_connection()
    try:
        # Load the SQL query
        sql_path = SQL_DIR / "user_sessions.sql"
        with open(sql_path, "r") as f:
            query = f.read()
        
        # Compute the minimum timestamp for filtering (timezone-aware UTC)
        min_timestamp = datetime.now(timezone.utc) - timedelta(days=days)
        # Convert to naive datetime for DuckDB (removes timezone info)
        # DuckDB stores TIMESTAMP as naive and interprets timezone-aware inputs as local time
        min_timestamp_naive = min_timestamp.replace(tzinfo=None)
        
        # Execute with parameterization: min_timestamp and limit
        result = conn.execute(
            query,
            [min_timestamp_naive, limit]
        ).fetchall()
        
        # Convert results to list of dictionaries for consistency
        if not result:
            return []
        
        # Get column names from the query result description
        columns = [desc[0] for desc in conn.description]
        rows = [dict(zip(columns, row)) for row in result]
        
        return rows
    
    except Exception as e:
        logger.error(f"Error querying user sessions: {e}")
        raise
    finally:
        conn.close()