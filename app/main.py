from fastapi import BackgroundTasks, FastAPI, Query, HTTPException
from fastapi.encoders import jsonable_encoder

from app.ingest import ingest_events
from app.load import load_all_events
from app import analytics

app = FastAPI()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/ingest")
def ingest(background_tasks: BackgroundTasks):
    """Trigger GitHub events ingestion in the background."""
    background_tasks.add_task(ingest_events)
    return {"status": "accepted", "message": "Ingestion started in background"}


@app.post("/load")
def load():
    """Load JSONL files from data/raw/ into DuckDB with idempotent deduplication."""
    result = load_all_events()
    return {
        "scanned_files": result.scanned_files,
        "loaded_files": result.loaded_files,
        "skipped_files": result.skipped_files,
        "inserted_events": result.inserted_events,
        "duration_ms": result.duration_ms,
        "db_path": result.db_path
    }


@app.get("/top-repos")
def get_top_repos(days: int = Query(7, gt=0), limit: int = Query(10, gt=0)):
    """Get top repositories by event count within a time window.
    
    Query parameters:
      - days: Number of days back to include (default 7, must be > 0).
      - limit: Maximum number of repositories to return (default 10, must be > 0).
    
    Returns:
      JSON list of repositories with event statistics.
    """
    try:
        result = analytics.get_top_repos(days=days, limit=limit)
        return jsonable_encoder(result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal error")


@app.get("/user-sessions")
def get_user_sessions(days: int = Query(7, gt=0), limit: int = Query(50, gt=0)):
    """Get user sessions based on activity patterns.
    
    A session is defined as a sequence of events for a user with no more
    than 30 minutes of inactivity between consecutive events.
    
    Query parameters:
      - days: Number of days back to include (default 7, must be > 0).
      - limit: Maximum number of sessions to return (default 50, must be > 0).
    
    Returns:
      JSON list of sessions with activity details.
    """
    try:
        result = analytics.get_user_sessions(days=days, limit=limit)
        return jsonable_encoder(result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal error")
