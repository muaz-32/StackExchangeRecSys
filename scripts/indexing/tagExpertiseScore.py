import json
import pandas as pd
from collections import defaultdict
from sklearn.preprocessing import MinMaxScaler

# --- File paths ---
ROOT_DIR = "../.."
input_files = {
    "accepted": f"{ROOT_DIR}/output/features/users_accepted_answers_tags.json",
    "answers": f"{ROOT_DIR}/output/features/users_answers_tags.json",
    "comments": f"{ROOT_DIR}/output/features/users_comments_tags.json",
    "questions": f"{ROOT_DIR}/output/features/users_questions_tags.json",
    "badges": f"{ROOT_DIR}/output/features/users_tag_badges.json"
}
output_file = f"{ROOT_DIR}/output/topic_expertise_scores.json"

# --- Load JSON files ---
def load_json(filename):
    with open(filename, 'r') as f:
        return json.load(f)

accepted = load_json(input_files["accepted"])
answers = load_json(input_files["answers"])
comments = load_json(input_files["comments"])
questions = load_json(input_files["questions"])
badges = load_json(input_files["badges"])

# --- Convert to (user, tag, count) DataFrames ---
def json_to_df(data, col_name):
    rows = []
    for user_id, tags in data.items():
        try:
            uid = int(user_id)
        except ValueError:
            continue  # skip keys like "UserId"
        tag_counts = defaultdict(int)
        for tag in tags:
            tag_counts[tag] += 1
        for tag, count in tag_counts.items():
            rows.append((uid, tag, count))
    return pd.DataFrame(rows, columns=["user_id", "tag", col_name])

df_accepted = json_to_df(accepted, "accepted")
df_answers = json_to_df(answers, "answers")
df_comments = json_to_df(comments, "comments")
df_questions = json_to_df(questions, "questions")

# --- Badge processing ---
badge_weights = {'gold': 3, 'silver': 2, 'bronze': 1}

badge_rows = []
for user_id, badge_list in badges.items():
    for badge in badge_list:
        if len(badge) != 3:
            continue  # skip malformed entries
        tag, rank, count = badge
        try:
            score = badge_weights.get(rank.lower(), 0) * int(count)
            badge_rows.append((int(user_id), tag, score))
        except ValueError:
            continue  # skip entries with non-integer count

df_badges = pd.DataFrame(badge_rows, columns=["user_id", "tag", "badge_score"])
df_badges = df_badges.groupby(["user_id", "tag"]).sum().reset_index()

# --- Merge all dataframes ---
dfs = [df_accepted, df_answers, df_comments, df_questions, df_badges]
df_final = df_accepted

for df in dfs[1:]:
    df_final = pd.merge(df_final, df, on=["user_id", "tag"], how="outer")

df_final = df_final.fillna(0).infer_objects(copy=False)

# --- Normalize each metric ---
scaler = MinMaxScaler()
score_columns = ["accepted", "answers", "comments", "questions", "badge_score"]
df_final[score_columns] = scaler.fit_transform(df_final[score_columns])

# --- Weights ---
weights = {
    "accepted": 0.4,
    "answers": 0.3,
    "comments": 0.1,
    "questions": 0.1,
    "badge_score": 0.1
}

# --- Calculate weighted expertise score ---
df_final["expertise_score"] = sum(df_final[col] * weight for col, weight in weights.items())

# --- Format as JSON dict ---
expertise_dict = defaultdict(list)

for _, row in df_final.iterrows():
    user_id = str(int(row["user_id"]))
    expertise_dict[user_id].append({
        "tag": row["tag"],
        "expertise_score": round(row["expertise_score"], 4)
    })

# --- Save to JSON ---
with open(output_file, "w") as f:
    json.dump(expertise_dict, f, indent=4)

print(f"✅ Expertise scores saved to '{output_file}'")
