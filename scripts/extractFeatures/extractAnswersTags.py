from collections import defaultdict
import csv
import json

# Root directory
ROOT_DIR = "../.."

# Input and output file paths
csv_file = f"{ROOT_DIR}/output/dump/users.answers.table.csv"
json_file = f"{ROOT_DIR}/output/features/users_answers_tags.json"

# Initialize a dictionary to store answered tags
users_answered_tags = defaultdict(list)

# Read the CSV file
with open(csv_file, mode='r', encoding='utf-8') as file:
    reader = csv.reader(file)
    for row in reader:
        # Assuming the answered tags are in the 9th column (index 8)
        user_id = row[0]
        answered_tag = row[8]
        if answered_tag:  # Ensure the tag is not empty
            users_answered_tags[user_id].append(answered_tag)

# Write the answered tags to a JSON file
with open(json_file, mode='w', encoding='utf-8') as file:
    json.dump({key: list(value) for key, value in users_answered_tags.items()}, file, ensure_ascii=False, indent=4)