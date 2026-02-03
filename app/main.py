from fastapi import BackgroundTasks, FastAPI

from app.ingest import ingest_events
from app.load import load_all_events

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
