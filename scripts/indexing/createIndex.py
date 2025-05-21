import faiss
import numpy as np
import pickle
import json
from functools import reduce
import pandas as pd

# File paths
ROOT_DIR = "../.."  # Adjust the root directory as needed

# Step 1: Extract behavioral features
def extract_behavioral_features(user_data):
    """
    Extract behavioral features for a user.
    """
    return [
        user_data.get("reputation", 0),
        user_data.get("gold_badges", 0),
        user_data.get("silver_badges", 0),
        user_data.get("bronze_badges", 0),
        user_data.get("accepted_answers_count", 0),
        user_data.get("total_answers_count", 0),
        user_data.get("answer_acceptance_ratio", 0.0),
        user_data.get("comment_count", 0),
        np.mean(user_data.get("question_scores", [0])),
        np.mean(user_data.get("answer_scores", [0])),
    ]

# Step 2: Extract topical features
def extract_topical_features():
    """
    Extract topical features for a user by reading data from JSON files.
    """
    # File paths for the JSON files
    accepted_answers_file = f"{ROOT_DIR}/output/features/users_accepted_answers_tags.json"
    answers_file = f"{ROOT_DIR}/output/features/users_answers_tags.json"
    comments_file = f"{ROOT_DIR}/output/features/users_comments_tags.json"
    questions_file = f"{ROOT_DIR}/output/features/users_questions_tags.json"
    tag_badges_file = f"{ROOT_DIR}/output/features/users_tag_badges.json"

    # Load data from JSON files
    with open(accepted_answers_file, "r", encoding="utf-8") as f:
        accepted_answers_tags_data = json.load(f)
    with open(answers_file, "r", encoding="utf-8") as f:
        answers_tags_data = json.load(f)
    with open(comments_file, "r", encoding="utf-8") as f:
        comments_tags_data = json.load(f)
    with open(questions_file, "r", encoding="utf-8") as f:
        questions_tags_data = json.load(f)
    with open(tag_badges_file, "r", encoding="utf-8") as f:
        tag_badges_data = json.load(f)

    # Define weights for "badge name", "badge rank", and "award count"
    weights = {"badge_name": 0.4, "badge_rank": 0.3, "award_count": 0.3}

    # Flatten the JSON structure and calculate the weighted sum
    flattened_data = []
    for user_id, badge_info in tag_badges_data.items():
        weighted_sum = 0
        for badge in badge_info:
            badge_name_weight = len(badge[0]) * weights["badge_name"]  # Example: weight by string length
            badge_rank_weight = {"gold": 3, "silver": 2, "bronze": 1}.get(badge[1].lower(), 0) * weights["badge_rank"]
            award_count_weight = int(badge[2]) * weights["award_count"]
            weighted_sum += badge_name_weight + badge_rank_weight + award_count_weight
        flattened_data.append({"user_id": user_id, "weighted_value": weighted_sum})

    # Convert the data to dataframes
    df_tag_badges = pd.DataFrame(tag_badges_data).T.reset_index()
    df_tag_badges.columns = ["user_id", "tag_badges"]
    df_tags_answered = pd.DataFrame(answers_tags_data).T.reset_index()
    df_tags_answered.columns = ["user_id", "tags_answered"]
    df_tags_accepted = pd.DataFrame(accepted_answers_tags_data).T.reset_index()
    df_tags_accepted.columns = ["user_id", "tags_accepted"]
    df_tags_commented = pd.DataFrame(comments_tags_data).T.reset_index()
    df_tags_commented.columns = ["user_id", "tags_commented"]
    df_tags_asked = pd.DataFrame(questions_tags_data).T.reset_index()
    df_tags_asked.columns = ["user_id", "tags_asked"]

    # Add a weight column to each DataFrame
    weights = {
        "tag_badges": 3,
        "tags_answered": 3,
        "tags_accepted": 5,
        "tags_commented": 1,
        "tags_asked": 1,
    }
    df_tag_badges["weight"] = weights["tag_badges"]
    df_tags_answered["weight"] = weights["tags_answered"]
    df_tags_accepted["weight"] = weights["tags_accepted"]
    df_tags_commented["weight"] = weights["tags_commented"]
    df_tags_asked["weight"] = weights["tags_asked"]

    # List of DataFrames
    frames = [df_tag_badges, df_tags_answered, df_tags_accepted, df_tags_commented, df_tags_asked]

    # Merge all DataFrames using reduce
    merged_df = reduce(lambda left, right: pd.concat([left, right], ignore_index=True), frames)

    # Print the merged DataFrame for debugging
    print("Merged DataFrame:")
    print(merged_df)

# Step 3: Convert user to vector
def user_to_vector(user_data):
    """
    Combine behavioral and topical features into a dense vector.
    """
    behavioral_features = extract_behavioral_features(user_data)
    topical_features = extract_topical_features(user_data)
    return np.array(behavioral_features + topical_features, dtype=np.float32)

# Step 4: Create FAISS index
def create_faiss_index(user_data):
    """
    Create a FAISS index for user data.
    """
    user_vectors = []
    user_ids = []
    for user in user_data:
        user_id = user["user_id"]
        user_vector = user_to_vector(user)
        user_vectors.append(user_vector)
        user_ids.append(user_id)

    # Create FAISS index
    dimension = len(user_vectors[0])  # Vector dimension
    index = faiss.IndexFlatL2(dimension)  # L2 distance metric
    index.add(np.array(user_vectors))  # Add vectors to the index

    return index, user_ids

# Step 5: Save FAISS index and user IDs
def save_faiss_index(index, user_ids, index_file, ids_file):
    """
    Save the FAISS index and user IDs to disk.
    """
    faiss.write_index(index, index_file)
    with open(ids_file, "wb") as f:
        pickle.dump(user_ids, f)

# Step 6: Load FAISS index and user IDs
def load_faiss_index(index_file, ids_file):
    """
    Load the FAISS index and user IDs from disk.
    """
    index = faiss.read_index(index_file)
    with open(ids_file, "rb") as f:
        user_ids = pickle.load(f)
    return index, user_ids

# Main function to orchestrate the process
if __name__ == "__main__":
    # Example user data
    user_data = [
        {
            "user_id": 1,
            "reputation": 100,
            "gold_badges": 2,
            "silver_badges": 5,
            "bronze_badges": 10,
            "accepted_answers_count": 15,
            "total_answers_count": 20,
            "answer_acceptance_ratio": 0.75,
            "comment_count": 50,
            "question_scores": [5, 10, 15],
            "answer_scores": [10, 20, 30],
            "tag_badges": {"python": 5, "keras": 3},
            "tags_answered": [1, 2],
            "tags_accepted_answers": [1],
            "tags_commented": [2],
            "tags_asked": [3],
        },
        # Add more user data here
    ]

    # File paths
    index_file = "user_index.faiss"
    ids_file = "user_ids.pkl"

    # Create and save FAISS index
    index, user_ids = create_faiss_index(user_data)
    save_faiss_index(index, user_ids, index_file, ids_file)

    print(f"FAISS index saved to {index_file} and user IDs saved to {ids_file}")