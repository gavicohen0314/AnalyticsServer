import pytest
from fastapi.testclient import TestClient
from datetime import datetime

from main import app, create_table_if_not_exists, get_connection


@pytest.fixture(scope="session", autouse=True)
def setup_database():
    """
    Fixture that runs once per test session to ensure the database table is created.
    """
    create_table_if_not_exists()


def test_process_event_inserts_into_db():
    client = TestClient(app)

    # 1. Send a POST request to /process_event
    payload = {
        "userid": "test_user",
        "eventname": "test_event"
    }
    response = client.post("/process_event", json=payload)

    # 2. Assert the response status code
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert data["userid"] == "test_user"
    assert data["eventname"] == "test_event"
    assert "eventtimestamputc" in data  # make sure we returned the timestamp

    # 3. Check if the data is indeed in the DB
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT eventtimestamputc, userid, eventname FROM events WHERE userid=? AND eventname=?", ("test_user", "test_event"))
    row = cursor.fetchone()
    conn.close()

    assert row is not None, "No row returned from DB"
    event_timestamp_str, userid, eventname = row
    assert userid == "test_user"
    assert eventname == "test_event"
    datetime.fromisoformat(event_timestamp_str)
