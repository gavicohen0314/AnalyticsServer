from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime, timezone, timedelta
import sqlite3
from contextlib import asynccontextmanager

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


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ---- Startup actions ----
    create_table_if_not_exists()
    yield


app = FastAPI(lifespan=lifespan)


class EventInput(BaseModel):
    userid: str
    eventname: str


class ReportInput(BaseModel):
    lastseconds: int
    userid: str


class EventOutput(BaseModel):
    eventtimestamputc: str
    userid: str
    eventname: str


class ReportOutput(BaseModel):
    status: str
    events: list[EventOutput]


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


@app.post("/get_reports")
def get_reports(report_data: ReportInput) -> ReportOutput:
    """
    Endpoint to fetch all events for a specific user ID that occurred within the last
    X seconds.
    """
    try:
        # Calculate the timestamp X seconds ago
        time_threshold = datetime.now(timezone.utc) - timedelta(seconds=report_data.lastseconds)
        time_threshold_str = time_threshold.isoformat()

        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT eventtimestamputc, userid, eventname
            FROM events
            WHERE userid = ? AND eventtimestamputc >= ?
            """,
            (report_data.userid, time_threshold_str)
        )

        rows = cursor.fetchall()
        conn.close()

        # Transform the rows into a list of dictionaries
        events = [
            {"eventtimestamputc": row[0], "userid": row[1], "eventname": row[2]}
            for row in rows
        ]

        return {"status": "success", "events": events}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
