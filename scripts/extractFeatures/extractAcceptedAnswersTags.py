from collections import defaultdict
import csv
import json

# Root directory
ROOT_DIR = "../.."

# Input and output file paths
csv_file = f"{ROOT_DIR}/output/dump/users.answers.table.csv"
json_file = f"{ROOT_DIR}/output/features/users_accepted_answers_tags.json"

# Initialize a dictionary to store accepted answers tags
users_accepted_answers_tags = defaultdict(set)

# Read the CSV file
with open(csv_file, mode='r', encoding='utf-8') as file:
    reader = csv.reader(file)
    for row in reader:
        # Assuming the IsAcceptedAnswer column is in the 4th column (index 3)
        if row[3] == "True":
            # Assuming the answers tags are in the 9th column (index 8)
            user_id = row[0]
            accepted_tag = row[8]
            if accepted_tag:  # Ensure the tag is not empty
                users_accepted_answers_tags[user_id].add(accepted_tag)

# Write the accepted answers tags to a JSON file
with open(json_file, mode='w', encoding='utf-8') as file:
    json.dump({key: list(value) for key, value in users_accepted_answers_tags.items()}, file, ensure_ascii=False, indent=4)