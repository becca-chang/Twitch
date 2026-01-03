import os
import pandas as pd
from pathlib import Path

DATA_ROOT = "data"
CLIP_DIRECTORY = f"{DATA_ROOT}/clips"
CHAT_DIRECTORY = f"{DATA_ROOT}/comments"
VIDEO_DIRECTORY = f"{DATA_ROOT}/videos"
MP4_DIRECTORY = f"{DATA_ROOT}/mp4"
CHAT_CSV_DIRECTORY = f"{DATA_ROOT}/comments_csv"


def create_comment_report(messaged_re_dir):
    # NEED TO CHECK
    user_reports_list = []
    for file in os.listdir(messaged_re_dir):
        user_id = file.split(".")[0]
        if user_id.isdigit():
            full_file_path = os.path.join(messaged_re_dir, file)
            df = pd.read_csv(full_file_path, index_col=0)

            message_count = df["message_id"].count()
            distinct_clip_count = df["clip_id"].nunique()
            subscribed_count = df[df["comment_type"] == 1]["tier_level"].count()
            gifting_count = df[df["comment_type"] == 2]["gifting_count"].count()
            gifting_amount = int(df[df["comment_type"] == 2]["gifting_count"].sum())
            cheer_count = df[df["comment_type"] == 3]["message"].count()
            cheer_amount = int(df[df["comment_type"] == 3]["cheer"].sum())
            user_report = {
                "user_id": user_id,
                "message_count": message_count,
                "distinct_clip_count": distinct_clip_count,
                "subscribed_count": subscribed_count,
                "gifting_count": gifting_count,
                "gifting_amount": gifting_amount,
                "cheer_count": cheer_count,
                "cheer_amount": cheer_amount,
            }
            user_reports_list.append(user_report)
    report_df = pd.DataFrame(data=user_reports_list)
    report_df.to_csv("data/reports.csv")


def get_user_clip_info(user_id):
    """Conut the number of clips given user id

    Args:
        user_id (int): user id

    Returns:
        dictionary: count of clips
    """

    user_df = pd.read_csv(f"{CLIP_DIRECTORY}/{user_id}.csv")
    count_of_clips = len(user_df)
    clips_df_with_video_id = user_df[user_df["video_id"].notna()]
    count_of_clips_with_video_id = len(clips_df_with_video_id)
    duration = float(clips_df_with_video_id["duration"].sum())

    return {
        "count_of_clips": count_of_clips,
        "count_of_clips_with_video_id": count_of_clips_with_video_id,
        "duration": duration,
    }


def make_clips_report():
    rows = []
    files = list(Path(CLIP_DIRECTORY).glob("*.csv"))
    for user_file in files:
        user_id = user_file.stem
        if user_id.isdigit():
            user_clips = get_user_clip_info(user_id)
            user_clips["user_id"] = user_id
            rows.append(user_clips)
    report = pd.DataFrame(rows)
    report.to_csv(f"{CLIP_DIRECTORY}/reports.csv", index=False)


if __name__ == "__main__":
    print(make_clips_report())
