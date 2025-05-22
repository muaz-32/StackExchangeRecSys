from collections import defaultdict
import csv
import json

# Root directory
ROOT_DIR = "../.."

# Input and output file paths
csv_file = f"{ROOT_DIR}/output/api/tag_based_badges.csv"
json_file = f"{ROOT_DIR}/output/features/users_tag_badges.json"

# Initialize a dictionary to store user tags
user_tags = defaultdict(list)

# Read the CSV file
with open(csv_file, mode='r', encoding='utf-8') as file:
    reader = csv.reader(file)
    for row in reader:
        user_id = row[0]
        # Assuming the tags are in the 2nd column (index 1)
        badge = row[1]
        rank = row[2]
        count = row[3]
        if badge:  # Ensure the tag is not empty
            # Add the badge, rank and count to the user_tags
            user_tags[user_id].append((badge, rank, count))

# Write the user_tags in a file
with open(json_file, mode='w', encoding='utf-8') as file:
    json.dump({key: list(value) for key, value in user_tags.items()}, file, ensure_ascii=False, indent=4)