from collections import defaultdict
import csv
import json

# Root directory
ROOT_DIR = "../.."

# Input and output file paths
csv_file = f"{ROOT_DIR}/output/dump/users.comments.table.csv"
json_file = f"{ROOT_DIR}/output/features/users_comments_tags.json"

# Initialize a dictionary to store comments tags
users_comments_tags = defaultdict(set)

# Read the CSV file
with open(csv_file, mode='r', encoding='utf-8') as file:
    reader = csv.reader(file)
    for row in reader:
        # Assuming the comments tags are in the 5th column (index 4)
        user_id = row[0]
        comment_tag = row[4]
        if comment_tag:  # Ensure the tag is not empty
            users_comments_tags[user_id].add(comment_tag)

# Write the comments tags to a JSON file
with open(json_file, mode='w', encoding='utf-8') as file:
    json.dump({key: list(value) for key, value in users_comments_tags.items()}, file, ensure_ascii=False, indent=4)