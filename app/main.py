from fastapi import BackgroundTasks, FastAPI

from app.ingest import ingest_events

app = FastAPI()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/ingest")
def ingest(background_tasks: BackgroundTasks):
    """Trigger GitHub events ingestion in the background."""
    background_tasks.add_task(ingest_events)
    return {"status": "accepted", "message": "Ingestion started in background"}
