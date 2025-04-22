import os
import csv
import requests
import time
from datetime import datetime
from itertools import cycle

# Base URL for the Stack Exchange API
BASE_URL = "https://api.stackexchange.com/2.3"

# Root directory
ROOT_DIR = "../.."

# Set the Stack Exchange site
SITE = "genai.stackexchange.com"

# List of API keys
API_KEYS = ["rl_AcJgSUjNrKYeV8cR7CZX7jXKL",
            "rl_dR5mDxPc3G6gLj5AQZofBsd6R",
            "rl_o6qEBMddrQcJi5P8AjvD9oxkD",
            "rl_WiALEvcddrpyt74t5as3uGzKJ",
            "rl_6PzmZqnSX4VEZfUmJJnf9uihW",
            "rl_xin44SwFnyccmjgQU4a9tT7xP",
            "rl_uUxqzkvKeCMiT3JDtX8fJdvkr"]

api_key_pool = cycle(API_KEYS)  # Rotate API keys

# Function to fetch all users from genai.stackexchange.com
def fetch_users(site=SITE, page_size=100):
    users = []
    page = 1
    request_count = 0
    max_requests_per_key = 10000 // len(API_KEYS)  # Distribute requests across keys
    current_key = next(api_key_pool)

    while True:
        print(f"[{datetime.now()}] Sending request {request_count + 1} to fetch page {page} of users...")
        response = requests.get(
            f"{BASE_URL}/users",
            params={
                "site": site,
                "pagesize": page_size,
                "page": page,
                "order": "desc",
                "sort": "reputation",
                "key": current_key
            }
        )
        request_count += 1

        # Handle rate limiting and backoff
        if response.status_code == 429:  # Too many requests
            print(f"[{datetime.now()}] Rate limit exceeded. Switching API key...")
            current_key = next(api_key_pool)
            time.sleep(1)  # Short delay before retrying
            continue

        data = response.json()

        # Handle backoff
        if "backoff" in data:
            backoff_time = data["backoff"]
            print(f"[{datetime.now()}] Backoff received. Waiting for {backoff_time} seconds...")
            time.sleep(backoff_time)

        if "items" not in data or not data["items"]:
            print(f"[{datetime.now()}] No more users to fetch. Total requests sent: {request_count}.")
            break

        users.extend(data["items"])
        print(f"[{datetime.now()}] Fetched {len(data['items'])} users from page {page}. Total users so far: {len(users)}.")

        if not data.get("has_more", False):
            print(f"[{datetime.now()}] No more pages available. Total requests sent: {request_count}.")
            break

        # Rotate API key if max requests per key are reached
        if request_count % max_requests_per_key == 0:
            current_key = next(api_key_pool)
            print(f"[{datetime.now()}] Switching to next API key...")

        # Rate limiting: Stay below 25 requests per second
        time.sleep(0.04)

        page += 1

    return users

# Function to generate a CSV table for users
def generate_users_table(output_file):
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    print(f"[{datetime.now()}] Starting to fetch users...")
    users = fetch_users()
    print(f"[{datetime.now()}] Finished fetching users. Total users fetched: {len(users)}.")

    with open(output_file, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        # Write header row
        writer.writerow([
            "user id", "user name", "gold badge count", "silver badge count", "bronze badge count",
            "view_count", "down_vote_count", "up_vote_count", "answer_count", "question_count",
            "account_id", "last_modified_date", "last_access_date", "reputation_change_year",
            "reputation_change_quarter", "reputation_change_month", "reputation_change_week",
            "reputation_change_day", "reputation", "creation_date", "user_type", "accept_rate",
            "about_me", "location", "display_name", "age"
        ])
        print(f"[{datetime.now()}] Header row written to CSV.")

        for index, user in enumerate(users, start=1):
            writer.writerow([
                user["user_id"],
                user.get("display_name", "Unknown"),
                user.get("badge_counts", {}).get("gold", 0),
                user.get("badge_counts", {}).get("silver", 0),
                user.get("badge_counts", {}).get("bronze", 0),
                user.get("view_count", 0),
                user.get("down_vote_count", 0),
                user.get("up_vote_count", 0),
                user.get("answer_count", 0),
                user.get("question_count", 0),
                user.get("account_id", "null"),
                user.get("last_modified_date", "null"),
                user.get("last_access_date", "null"),
                user.get("reputation_change_year", 0),
                user.get("reputation_change_quarter", 0),
                user.get("reputation_change_month", 0),
                user.get("reputation_change_week", 0),
                user.get("reputation_change_day", 0),
                user.get("reputation", 0),
                user.get("creation_date", "null"),
                user.get("user_type", "null"),
                user.get("accept_rate", "null"),
                user.get("about_me", "null"),
                user.get("location", "null"),
                user.get("display_name", "Unknown"),
                user.get("age", "null")
            ])
            if index % 10 == 0:  # Log progress every 10 users
                print(f"[{datetime.now()}] Written {index} users to the CSV file.")

    print(f"[{datetime.now()}] User table generated successfully at '{output_file}'.")

# Run the script
if __name__ == "__main__":
    generate_users_table(output_file=f"{ROOT_DIR}/output/api/users.csv")