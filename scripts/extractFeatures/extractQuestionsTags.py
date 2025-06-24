from collections import defaultdict
import csv
import json

# Root directory
ROOT_DIR = "../.."

# Input and output file paths
csv_file = f"{ROOT_DIR}/output/dump/users.questions.table.csv"
json_file = f"{ROOT_DIR}/output/features/users_questions_tags.json"

# Initialize a dictionary to store user tags
user_tags = defaultdict(list)

# Read the CSV file
with open(csv_file, mode='r', encoding='utf-8') as file:
    reader = csv.reader(file)
    for row in reader:
        # Assuming the tags are in the 8th column (index 7)
        tag = row[7]
        if tag:  # Ensure the tag is not empty
            user_tags[row[0]].append(tag)

# Write the user_tags in a file
with open(json_file, mode='w', encoding='utf-8') as file:
    json.dump({key: list(value) for key, value in user_tags.items()}, file, ensure_ascii=False, indent=4)