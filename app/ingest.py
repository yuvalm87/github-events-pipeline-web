import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Generator

import requests

logger = logging.getLogger(__name__)

GITHUB_EVENTS_URL = "https://api.github.com/events"
BATCH_SIZE = 150
DATA_DIR = Path(__file__).parent.parent / "data" / "raw"


def fetch_github_events() -> Generator[dict, None, None]:
    """Fetch GitHub events from the public events API."""
    try:
        response = requests.get(GITHUB_EVENTS_URL, timeout=10)
        response.raise_for_status()
        events = response.json()

        for event in events:
            yield event

    except requests.RequestException as e:
        logger.error(f"Failed to fetch GitHub events: {e}")
        raise


def add_ingestion_timestamp(event: dict) -> dict:
    """Add ingestion timestamp metadata to an event."""
    event["_ingested_at"] = datetime.utcnow().isoformat() + "Z"
    return event


def save_events_batch(batch: list[dict], batch_number: int) -> None:
    """Save a batch of events to a JSONL file."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = DATA_DIR / f"events_{timestamp}_batch_{batch_number:03d}.jsonl"

    try:
        with open(filename, "w", encoding="utf-8") as f:
            for event in batch:
                f.write(json.dumps(event, ensure_ascii=False) + "\n")
        logger.info(f"Saved {len(batch)} events to {filename}")
    except IOError as e:
        logger.error(f"Failed to save batch to {filename}: {e}")
        raise


def ingest_events() -> None:
    """Fetch GitHub events and save them in batches."""
    try:
        batch = []
        batch_number = 0
        total_events = 0

        for event in fetch_github_events():
            event_with_timestamp = add_ingestion_timestamp(event)
            batch.append(event_with_timestamp)

            if len(batch) >= BATCH_SIZE:
                save_events_batch(batch, batch_number)
                total_events += len(batch)
                batch_number += 1
                batch = []

        # Save remaining events
        if batch:
            save_events_batch(batch, batch_number)
            total_events += len(batch)

        logger.info(f"Ingestion complete: {total_events} events in {batch_number + 1} batches")

    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
