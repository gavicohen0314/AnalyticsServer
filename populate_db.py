import httpx
import random
import string
from joblib import Parallel, delayed

ANALYTICS_SERVER_URL = "https://analytics-server-frdaa5eecqc3acdy.israelcentral-01.azurewebsites.net/process_event"


# Function to generate random data
def generate_random_event():
    userid = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    eventname = ''.join(random.choices(string.ascii_lowercase, k=6))
    return {"userid": userid, "eventname": eventname}


# Function to send an HTTP POST request
def send_event():
    event_data = generate_random_event()
    try:
        response = httpx.post(ANALYTICS_SERVER_URL, json=event_data)
        if response.status_code == 200:
            print(f"Success: {event_data}")
        else:
            print(f"Failed: {event_data}, Status Code: {response.status_code}")
    except Exception as e:
        print(f"Error sending event: {e}")


# Main function to make 1000 parallel requests
def main():
    print("Starting to send events...")
    Parallel(n_jobs=-1)(delayed(send_event)() for _ in range(1000))
    print("Finished sending events.")


if __name__ == "__main__":
    main()
