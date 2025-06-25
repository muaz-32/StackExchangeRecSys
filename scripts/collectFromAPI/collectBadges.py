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

# # Proxies
# proxies = {
#     "http": "http://47.90.167.27:8081",
#     "https": "http://47.90.167.27:8081"
# }

# Function to fetch all badges for a specific user
def fetch_user_badges(user_id, site=SITE):
    current_key = next(api_key_pool)
    response = requests.get(
        f"{BASE_URL}/users/{user_id}/badges",
        params={
            "site": site,
            "order": "desc",
            "sort": "rank",
            "key": current_key,
            "pagesize": 100  # Fetch up to 100 badges per request
        },
        # proxies=proxies  # Use the specified proxies
    )
    if response.status_code == 429:  # Too many requests
        print(f"[{datetime.now()}] Rate limit exceeded. Switching API key...")
        time.sleep(1)  # Short delay before retrying
        return fetch_user_badges(user_id, site)  # Retry with a new key
    data = response.json()
    if "backoff" in data:
        backoff_time = data["backoff"]
        print(f"[{datetime.now()}] Backoff received. Waiting for {backoff_time} seconds...")
        time.sleep(backoff_time)
    return data.get("items", [])  # Fetch all badges

# Main function to fetch badges and write them into two CSV files
def generate_badges_tables(users_file, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    # File paths for the two tables
    tag_based_file = os.path.join(output_dir, "tag_based_badges.csv")
    named_file = os.path.join(output_dir, "named_badges.csv")

    # Initialize request counter
    request_count = 0

    # Open files for writing
    with open(tag_based_file, mode="w", newline="", encoding="utf-8") as tag_csv, \
            open(named_file, mode="w", newline="", encoding="utf-8") as named_csv:

        # CSV writers
        tag_writer = csv.writer(tag_csv)
        named_writer = csv.writer(named_csv)

        # Write headers
        tag_writer.writerow(["user id", "badge name", "badge rank", "award count", "creation date"])
        named_writer.writerow(["user id", "badge name", "badge rank", "award count", "creation date"])
        print(f"[{datetime.now()}] Headers written to both CSV files.")

        # Read users from the users file
        with open(users_file, mode="r", encoding="utf-8") as user_csv:
            reader = csv.DictReader(user_csv)
            for user_index, row in enumerate(reader, start=1):
                user_id = row["user id"]
                print(f"[{datetime.now()}] Fetching badges for user ID: {user_id} (User {user_index})...")

                # Fetch badges for the user
                badges = fetch_user_badges(user_id)
                request_count += 1
                print(f"[{datetime.now()}] Fetched {len(badges)} badges for user ID: {user_id}. Total requests so far: {request_count}.")

                # Write badges to the appropriate files
                for badge_index, badge in enumerate(badges, start=1):
                    badge_name = badge["name"]
                    badge_rank = badge["rank"]
                    award_count = badge["award_count"]
                    creation_date = badge.get("creation_date", "null")

                    # Write to the appropriate file based on badge type
                    if badge["badge_type"] == "tag_based":
                        tag_writer.writerow([user_id, badge_name, badge_rank, award_count, creation_date])
                    elif badge["badge_type"] == "named":
                        named_writer.writerow([user_id, badge_name, badge_rank, award_count, creation_date])

                    # Log progress every 10 badges
                    if badge_index % 10 == 0:
                        print(f"[{datetime.now()}] Written {badge_index} badges for user ID: {user_id}.")

                # Log progress every 5 users
                if user_index % 5 == 0:
                    print(f"[{datetime.now()}] Processed {user_index} users so far.")

                # Rate limiting: Stay below 25 requests per second
                # time.sleep(0.04)

    print(f"[{datetime.now()}] Tag-based badges written to: {tag_based_file}")
    print(f"[{datetime.now()}] Named badges written to: {named_file}")
    print(f"[{datetime.now()}] Total requests sent: {request_count}")

# Run the script
if __name__ == "__main__":
    generate_badges_tables(users_file=f"{ROOT_DIR}/output/api/users.csv", output_dir=f"{ROOT_DIR}/output/api")