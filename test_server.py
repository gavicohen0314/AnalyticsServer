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


def test_missing_userid():
    client = TestClient(app)

    # Missing 'userid' in payload
    payload = {
        "eventname": "test_event"
    }
    response = client.post("/process_event", json=payload)

    # Assert bad request response
    assert response.status_code == 422
    data = response.json()
    assert "detail" in data


def test_missing_eventname():
    client = TestClient(app)

    # Missing 'eventname' in payload
    payload = {
        "userid": "test_user"
    }
    response = client.post("/process_event", json=payload)

    # Assert bad request response
    assert response.status_code == 422
    data = response.json()
    assert "detail" in data


def test_empty_payload():
    client = TestClient(app)

    # Empty payload
    payload = {}
    response = client.post("/process_event", json=payload)

    # Assert bad request response
    assert response.status_code == 422
    data = response.json()
    assert "detail" in data


def test_invalid_userid_type():
    client = TestClient(app)

    # Invalid type for 'userid'
    payload = {
        "userid": 12345,  # Should be a string
        "eventname": "test_event"
    }
    response = client.post("/process_event", json=payload)

    # Assert bad request response
    assert response.status_code == 422
    data = response.json()
    assert "detail" in data


def test_invalid_eventname_type():
    client = TestClient(app)

    # Invalid type for 'eventname'
    payload = {
        "userid": "test_user",
        "eventname": ["invalid_list"]  # Should be a string
    }
    response = client.post("/process_event", json=payload)

    # Assert bad request response
    assert response.status_code == 422
    data = response.json()
    assert "detail" in data


def test_sql_injection_attempt():
    client = TestClient(app)

    # Payload attempting SQL injection
    malicious_userid = "test_user'; DROP TABLE events; --"
    payload = {
        "userid": malicious_userid,
        "eventname": "test_event"
    }
    response = client.post("/process_event", json=payload)

    # Assert that the request succeeds or fails gracefully
    assert response.status_code == 200, "SQL injection attempt caused failure"
    data = response.json()
    assert data["status"] == "success"
    assert data["userid"] == malicious_userid
    assert data["eventname"] == "test_event"

    # Verify the input is stored as-is in the database
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT userid, eventname FROM events WHERE userid=? AND eventname=?", (malicious_userid, "test_event"))
    row = cursor.fetchone()
    conn.close()

    assert row is not None, "Malicious input was not stored"
    stored_userid, stored_eventname = row
    assert stored_userid == malicious_userid, "Stored userid does not match input"
    assert stored_eventname == "test_event", "Stored eventname does not match input"

    # Check the table still exists
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='events'")
    table = cursor.fetchone()
    conn.close()
    assert table is not None, "Table 'events' was dropped!"


def test_extra_unexpected_fields():
    client = TestClient(app)

    # Payload with extra fields
    payload = {
        "userid": "test_user",
        "eventname": "test_event",
        "extra_field": "unexpected_value"
    }
    response = client.post("/process_event", json=payload)

    # Assert the system ignores extra fields and processes correctly
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert data["userid"] == "test_user"
    assert data["eventname"] == "test_event"
    assert "eventtimestamputc" in data
