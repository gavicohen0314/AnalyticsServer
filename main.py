from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime, timezone
import sqlite3

DATABASE_FILE = "events.db"


def get_connection() -> sqlite3.Connection:
    """
    Returns a connection to the SQLite database.
    If no path is provided, it uses the default DATABASE_FILE.
    """
    return sqlite3.connect(DATABASE_FILE)


def create_table_if_not_exists() -> None:
    """
    Creates the events table if it doesn't exist.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            eventtimestamputc TEXT NOT NULL,
            userid TEXT NOT NULL,
            eventname TEXT NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()


async def lifespan(app: FastAPI):
    # ---- Startup actions ----
    create_table_if_not_exists()


app = FastAPI(lifespan=lifespan)


class EventInput(BaseModel):
    userid: str
    eventname: str


@app.post("/process_event")
def process_event(event_data: EventInput) -> dict[str, str]:
    """
    Endpoint to process an event. Stores the current UTC time,
    user ID, and event name into the SQLite database.
    """
    event_timestamp = datetime.now(timezone.utc).isoformat()

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO events (eventtimestamputc, userid, eventname)
        VALUES (?, ?, ?)
        """,
        (event_timestamp, event_data.userid, event_data.eventname)
    )
    conn.commit()
    conn.close()

    return {
        "status": "success",
        "userid": event_data.userid,
        "eventname": event_data.eventname,
        "eventtimestamputc": event_timestamp
    }
