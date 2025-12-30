import demoji
import json
import os
import pandas as pd
import requests
import subprocess
import time

from typing import Union
from tqdm import tqdm

from chat_downloader import ChatDownloader
from chat_downloader.errors import NoChatReplay
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from utils.utils import *
from utils.process_file import read_or_create_csv_file, read_json_file

CLIENT_ID = "olj1zlf45mtffa1166zd8b1ersrew3"
AUTHORIZATION = "Bearer wm5mm5vlualx8xbpa6qumsne0crb33"
TWITCH_HEADERS = {"Client-Id": CLIENT_ID, "Authorization": AUTHORIZATION}

DATA_ROOT = "data"
CLIP_DIRECTORY = f"{DATA_ROOT}/clips"
CHAT_DIRECTORY = f"{DATA_ROOT}/comments"
VIDEO_DIRECTORY = f"{DATA_ROOT}/videos"
MP4_DIRECTORY = f"{DATA_ROOT}/mp4"
CHAT_CSV_DIRECTORY = f"{DATA_ROOT}/comments_csv"
CHAT_WITH_RE_DIR = os.path.join(CHAT_CSV_DIRECTORY, "chat_with_re")

USERS_INFO_FILE = f"{DATA_ROOT}/users_info.csv"

CHEER_PATTERN = r"Cheer(\d+)(?:\s|$)"
SUBSCRIBED_PATTERN = r"subscribed at Tier (\d+).*?(\d+|\w+) month"
GIFTING_PATTERN = r"gifting (\d+) Tier (\d+) Subs to (\w+)'s community"

CHAT_ERROR_LOG = f"{DATA_ROOT}/chat_error.log"
CHAT_TO_CSV_ERROR_LOG = f"{CHAT_DIRECTORY}/chats_to_df_errors.csv"
CHAT_IS_EMPTY_LOG = f"{CHAT_DIRECTORY}/chats_to_df_empty.csv"
CHAT_TO_CSV_ERROR_LOG_COLUMNS = ["datetime", "user_id", "file_path", "message"]
CHAT_IS_EMPTY_LOG_COLUMNS = ["datetime", "user_id", "file_path"]
RE_MESSAGE_LOG = f"{CHAT_DIRECTORY}/re_message.log"
FETCH_CLIPS_LOG = f"{CLIP_DIRECTORY}/fetch_data.log"
PROCESS_CHAT_CSV_LOG = f"{CHAT_CSV_DIRECTORY}/process_chat_csv.txt"
DOWNLOAD_MP4_LOG = f"{MP4_DIRECTORY}/download_mp4.txt"


class Twitch:
    def __init__(
        self, started_at: Optional[str] = None, ended_at: Optional[str] = None
    ):
        self.started_at = started_at
        self.ended_at = ended_at

    def get_users_by_login_names(self, names: list):

        url = make_url("https://api.twitch.tv/helix/users", "login", names)
        payload = {}
        response = requests.request(
            "GET", url, headers=TWITCH_HEADERS, data=payload
        ).json()
        return response

    def get_user_follower_count(self, user_id: str):
        url = "https://api.twitch.tv/helix/channels/followers"

        payload = {"broadcaster_id": user_id}
        response = requests.request("GET", url, headers=TWITCH_HEADERS, params=payload)

        return response.json().get("total", 0)

    def get_clip_info(
        self,
        user_id: str,
        started_at: Optional[str] = None,
        ended_at: Optional[str] = None,
    ):
        """
        Efficiently retrieve clip information with concurrent pagination

        :param user_id: Twitch user ID
        :param started_at: Start date for clips retrieval
        :param started_at: End date for clips retrieval
        :return: Dictionary of clip data
        """
        if not started_at:
            started_at = f"{(datetime.today()-timedelta(days=90)).strftime('%Y-%m-%d')}T00:00:00Z"
        if not ended_at:
            ended_at = f"{datetime.today().strftime('%Y-%m-%d')}T00:00:00Z"

        if started_at > ended_at:
            raise Exception("Time range is incorrect.")

        url = "https://api.twitch.tv/helix/clips"
        result = {"data": []}
        pagination = None

        def fetch_clips_page(payload):
            """Internal method to fetch a single page of clips"""
            try:
                response = requests.get(
                    url, headers=TWITCH_HEADERS, params=payload, timeout=10
                )
                response.raise_for_status()
                return response.json()
            except requests.RequestException as e:
                write_log(FETCH_CLIPS_LOG, f"Error fetching clips: {e}")
                return {"data": [], "pagination": {}}

        with ThreadPoolExecutor(max_workers=200) as executor:
            while True:
                # Prepare payload
                payload = {
                    "broadcaster_id": user_id,
                    "started_at": started_at,
                    "ended_at": ended_at,
                }
                if pagination:
                    payload["after"] = pagination

                # Execute request with rate limiting
                time.sleep(0.2)
                future = executor.submit(fetch_clips_page, payload)
                r_data = future.result()

                # Process retrieved data
                clips = r_data.get("data", [])
                result["data"].extend(clips)

                # Check pagination and clip limit
                pagination = r_data.get("pagination", {}).get("cursor")
                if not pagination:
                    break

        return result

    def summary_user_clips_to_csv(self, user: str):
        file_path = f"{CLIP_DIRECTORY}/{user}.csv"
        summary_clips = read_or_create_csv_file(file_path)
        data = self.get_clip_info(
            user, started_at=self.started_at, ended_at=self.ended_at
        ).get("data")
        if data:
            clip_summary = pd.DataFrame(data=data)
            clip_summary.rename(columns={"id": "clip_id"}, inplace=True)
            concat_df_to_file(
                [summary_clips, clip_summary], file_path, subset=["clip_id"]
            )
            return clip_summary
        else:
            write_log(FETCH_CLIPS_LOG, f"{user} has no clips")
            return pd.DataFrame()


class ChatDownload:
    def __init__(self):
        self.downloader = ChatDownloader()

    def download_and_save_chats_from_clips(
        self, user_id, output_directory: str, clip_urls: dict[str, str]
    ):
        os.makedirs(output_directory, exist_ok=True)
        clip_id_without_chat_replay = []
        clip_url_without_chat_replay = []

        file_names_without_extension = [
            os.path.splitext(file)[0]
            for file in os.listdir(output_directory)
            if os.path.isfile(os.path.join(output_directory, file))
        ]

        def process_clip(clip_id, clip_url):
            if clip_id in file_names_without_extension:
                return None
            try:
                chats = self.downloader.get_chat(clip_url)
                with open(
                    f"{output_directory}/{clip_id}.json", "w", encoding="utf-8"
                ) as f:
                    json.dump(list(chats), f, ensure_ascii=False, indent=4)
            except NoChatReplay:
                return (clip_id, clip_url)
            except Exception as e:
                exception_message = (
                    f"process_clip({clip_id},{clip_url}). Exception: {e}"
                )
                write_log(CHAT_ERROR_LOG, exception_message)
            return None

        # Using ThreadPoolExecutor to process clips in parallel
        with ThreadPoolExecutor() as executor:
            future_to_clip = {
                executor.submit(process_clip, clip_id, clip_url): clip_id
                for clip_id, clip_url in clip_urls.items()
            }

            for future in as_completed(future_to_clip):
                result = future.result()
                if result:
                    clip_id_without_chat_replay.append(result[0])
                    clip_url_without_chat_replay.append(result[1])

        df = pd.DataFrame(
            {
                "clip_id": clip_id_without_chat_replay,
                "clip_url": clip_url_without_chat_replay,
            }
        )
        df.to_csv(f"{CHAT_DIRECTORY}/{user_id}_clips_without_chat.csv", index=False)


def get_unique_values_from_df_column(df, column):
    df_clean = df[df[column].notna()]
    df_clean[column] = df_clean[column].astype(int, errors="ignore")
    unique_values = list(set(df_clean[column]))
    return unique_values


def create_users_info_file(data: list, user_info_file_path: str):
    user_info_df = pd.DataFrame(data=data)
    user_info_df.rename(
        columns={
            "id": "twitch_user_id",
        },
        inplace=True,
    )

    user_info_df["twitch_user_id"] = user_info_df["twitch_user_id"].astype(str)
    user_info_df.to_csv(user_info_file_path, index=False)
    return user_info_df


chatdownloader = ChatDownload()


def export_single_user_chats_to_csv(
    origin_file_path,
    user_id: str,
    chat_error_file_columns=CHAT_TO_CSV_ERROR_LOG_COLUMNS,
    chat_empty_file_columns=CHAT_IS_EMPTY_LOG_COLUMNS,
) -> str:
    """
    1. Read user's all chats file(.json) in "<chat_directory>/<user_id>".
    2. Write all of them into a csv file.
    Args:
        user (str): user id
        origin_file_path: json file
        chat_error_file_columns (list, optional):
            Defaults to ["datetime", "user_id", "file_path", "message"].
        chat_empty_file_columns (list, optional):
            Defaults to ["datetime", "user_id", "file_path"].

    Returns:
        df: chat file
    """
    return_dict = {}
    chat_error_df = read_or_create_csv_file(
        CHAT_TO_CSV_ERROR_LOG, columns=chat_error_file_columns
    )
    chat_empty_df = read_or_create_csv_file(
        CHAT_IS_EMPTY_LOG, columns=chat_empty_file_columns
    )
    # record error log
    chat_error_datetime = []
    chat_error_user = []
    chat_error_file_path = []
    chat_error_message = []
    empty = dict(zip(chat_empty_file_columns, [[], [], []]))

    author_id_list = []
    messages_list = []
    message_ids_list = []
    time_texts_list = []
    time_in_seconds_list = []
    clips_id_list = []
    chats_file_path_list = []
    badges_list = []
    os.makedirs(f"{CHAT_CSV_DIRECTORY}/{user_id}", exist_ok=True)
    if origin_file_path.endswith(".DS_Store"):
        return None
    if origin_file_path.endswith(".json"):
        clip_id = origin_file_path.split("/")[-1].split(".")[0]
        try:
            df_chat = read_json_file(
                origin_file_path
            )  # 'data/chats/100869214/MildBlindingEelFloof-RnekrluTMQ3PlSfh.json'
            if df_chat.empty:
                empty.get(chat_empty_file_columns[0]).append(
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                )
                empty.get(chat_empty_file_columns[1]).append(user_id)
                empty.get(chat_empty_file_columns[2]).append(clip_id)
            # author
            for i in df_chat["author"]:
                author_id_list.append(i.get("id"))

                badges_list.append(
                    [
                        badge.get("title") if badge.get("title") else []
                        for badge in i.get("badges", [])
                    ]
                )
            # message
            messages = df_chat["message"]
            messages_list.extend(messages)
            # message_ids
            message_ids = df_chat["message_id"]
            message_ids_list.extend(message_ids)
            # time_texts
            time_texts = df_chat["time_text"]
            time_texts_list.extend(time_texts)
            # time_in_seconds
            time_in_seconds = df_chat["time_in_seconds"]
            time_in_seconds_list.extend(time_in_seconds)
            # clip id
            clips_id_list.extend([clip_id for _ in range(len(df_chat))])
            # chat file path
            chats_file = [origin_file_path for _ in range(len(df_chat))]
            chats_file_path_list.extend(chats_file)
            clip_chat_df = pd.DataFrame(
                data={
                    "author_id": author_id_list,
                    "badges_list": badges_list,
                    "raw_message": messages_list,
                    "message_id": message_ids_list,
                    "time_text": time_texts_list,
                    "time_in_seconds": time_in_seconds_list,
                    "clip_id": clips_id_list,
                    "chats_file_path": chats_file_path_list,
                }
            )
            cleaned_clip_path = f"{CHAT_CSV_DIRECTORY}/{user_id}/{clip_id}.csv"
            clip_chat_df.to_csv(cleaned_clip_path)
            return_dict = {
                "clip_chat_df": clip_chat_df,
                "cleaned_clip_path": cleaned_clip_path,
            }
        except Exception as e:
            chat_error_datetime.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            chat_error_user.append(user_id)
            chat_error_file_path.append(origin_file_path)
            chat_error_message.append(e)
        errors_df = pd.DataFrame(
            dict(
                zip(
                    chat_error_file_columns,
                    [
                        chat_error_datetime,
                        chat_error_user,
                        chat_error_file_path,
                        chat_error_message,
                    ],
                )
            )
        )
        empty_df = pd.DataFrame(empty)
        errors_df = pd.concat([chat_error_df, errors_df], ignore_index=True)
        errors_df.to_csv(CHAT_TO_CSV_ERROR_LOG, index=False)
        empty_df = pd.concat([chat_empty_df, empty_df], ignore_index=True)
        empty_df.to_csv(CHAT_IS_EMPTY_LOG, index=False)
    return return_dict


def deal_with_badge(row):
    error_list = []
    badge_list = row["badges_list"]
    for badge in badge_list:
        try:
            # vip
            if "VIP" in badge:
                row["badge_is_vip"] = True
            # badge_premium_user
            if ("Prime Gaming" in badge) or ("Turbo" in badge):
                row["badge_premium_user"] = badge
            # subscriber
            if "Subscriber" in badge:  # e.g. 3-Month Subscriber
                row["badge_has_subscription_badge"] = True
                month = re.match(r"(\d+)", "Month Subscriber")
                if month:
                    row["badge_subscription_badge_month"] = int(month.group())
                else:
                    row["badge_subscription_badge_month"] = 1
            # gifter
            if "Gifter Leader" in badge:  # e.g. Gifter Leader 3
                row["badge_sub_gift_leader"] = badge.split(" ")[-1]
            if "Gift Subs" in badge:  # e.g. 10 Gift Subs
                row["badge_has_sub_gifter_badge"] = True
                row["badge_sub_gifter_badge_version"] = badge.split(" ")[0]
            # cheer
            if "cheer" in badge:  # e.g. cheer 5000
                row["badge_has_bits_badge"] = True
                row["badge_bits_badge_cheer"] = badge.split(" ")[-1]
            if "Bits Leader" in badge:  # e.g. Bits Leader 2
                row["badge_bits_leader"] = badge.split(" ")[-1]
        except Exception as e:
            error_list.append(row["message_id"])
    return row


# Regular expression message
def re_message(chat_df, column="raw_message", **kwargs):
    chat_df["comment_type"] = None
    # cheer
    chat_df["cheer_type"] = None
    chat_df["cheer"] = None
    # subscribe/gifting
    chat_df["self_subscribed_type"] = None
    chat_df["tier_level"] = None
    chat_df["subscribed_month"] = None
    chat_df["gifting_count"] = None
    # author badge
    chat_df["badge_has_bits_badge"] = False
    chat_df["badge_bits_badge_cheer"] = None
    chat_df["badge_bits_leader"] = None
    chat_df["badge_has_subscription_badge"] = None
    chat_df["badge_subscription_badge_month"] = None
    chat_df["badge_has_sub_gifter_badge"] = None
    chat_df["badge_sub_gifter_badge_version"] = None
    chat_df["badge_sub_gift_leader"] = None
    chat_df["badge_premium_user"] = None
    chat_df["badge_is_vip"] = None

    chat_df["re_message_error"] = None

    cheer_pattern = kwargs.get("cheer_pattern")
    subscribed_pattern = kwargs.get("subscribed_pattern")
    gifting_pattern = kwargs.get("gifting_pattern")
    messages = list(chat_df[column].astype(str))
    for index, message in enumerate(messages):
        try:
            # message
            if re.match(cheer_pattern, message):  # 小奇點
                chat_df.loc[index, "comment_type"] = 0
                chat_df.loc[index, "cheer_type"] = 1
                chat_df.loc[index, "cheer"] = re.match(cheer_pattern, message).group(1)
            elif re.search(subscribed_pattern, message):  # 自己訂閱
                chat_df.loc[index, "comment_type"] = 0
                chat_df.loc[index, "self_subscribed_type"] = 1
                chat_df.loc[index, "tier_level"] = re.search(
                    subscribed_pattern, message
                ).group(1)
                chat_df.loc[index, "subscribed_month"] = re.search(
                    subscribed_pattern, message
                ).group(2)
            elif re.search(gifting_pattern, message):  # 贈送訂閱
                chat_df.loc[index, "comment_type"] = 0
                chat_df.loc[index, "cheer_type"] = 0
                chat_df.loc[index, "self_subscribed_type"] = 0
                chat_df.loc[index, "tier_level"] = re.search(
                    gifting_pattern, message
                ).group(2)
                chat_df.loc[index, "gifting_count"] = re.search(
                    gifting_pattern, message
                ).group(1)
            else:
                chat_df.loc[index, "comment_type"] = 1
        except Exception as e:
            exception_message = f"""re_message(chat_df_index: {index}, chats_file_path: {chat_df['chats_file_path']}). Exception: {e}
            """
            write_log(RE_MESSAGE_LOG, exception_message)
            # chat_df.loc[index, "re_message_error"] = e
            continue
    return chat_df


def get_emoji_meaning(chat_df, column="raw_message"):
    chat_df["message"] = None
    messages = list(chat_df[column].astype(str))
    for index, message in enumerate(messages):
        emoji_desc = demoji.findall(message)  # {🔥: fire}

        emoji_list = [char for char in message if char in emoji_desc]
        chat_df.loc[index, "emoji_count"] = int(len(emoji_list))

        for emoji, emoji_meaning in emoji_desc.items():
            message = message.replace(emoji, emoji_meaning)
        chat_df.loc[index, "message"] = message

    return chat_df


def process_chat_csv(user_id: str) -> Union[dict, None]:
    """
    Process chat files for a single user with error handling
    """
    try:
        user_chat_dir = os.path.join(CHAT_DIRECTORY, str(user_id))
        results = []

        for file in os.listdir(user_chat_dir):
            try:
                origin_file_path = os.path.join(user_chat_dir, file)
                clip_chat_df = export_single_user_chats_to_csv(
                    origin_file_path, user_id
                )

                if clip_chat_df:
                    clip_df = clip_chat_df.get("clip_chat_df")
                    cleaned_clip_path = clip_chat_df.get("cleaned_clip_path")

                    chat_df_with_regex = re_message(
                        clip_df,
                        "raw_message",
                        **{
                            "cheer_pattern": CHEER_PATTERN,
                            "subscribed_pattern": SUBSCRIBED_PATTERN,
                            "gifting_pattern": GIFTING_PATTERN,
                        },
                    )

                    chat_df_with_emoji_meaning = get_emoji_meaning(
                        chat_df_with_regex, "raw_message"
                    )

                    chat_df_with_badge_info = chat_df_with_emoji_meaning.apply(
                        deal_with_badge, axis=1
                    )
                    chat_df_with_badge_info.to_csv(cleaned_clip_path, index=False)
                    results.append({"file": file, "status": "success"})
            except Exception as e:
                results.append(
                    {
                        "user_id": user_id,
                        "file": file,
                        "status": "error",
                        "error": str(e),
                    }
                )
                message = f"Error processing file {file} for user {user_id}: {str(e)}"
                write_log(PROCESS_CHAT_CSV_LOG, message)
        return {"user_id": user_id, "processed_files": results}
    except Exception as e:
        message = f"Error processing user {user_id}: {str(e)}"
        write_log(PROCESS_CHAT_CSV_LOG, message)
        return {"user_id": user_id, "status": "error", "error": str(e)}


def get_user_clips_without_chats(
    user_id: str, clip_directory: str, chat_directory: str
):
    # User clips summary
    clip_id_list = []
    full_path = os.path.join(clip_directory, f"{user_id}.csv")  # user's clips
    user_clips_summary = pd.read_csv(full_path)  # user's clips
    clip_id_list = user_clips_summary["id"]

    chats_download = f"{chat_directory}/{user_id}"
    if os.path.exists(chats_download):
        if not os.listdir(chats_download):
            lost_chat_clips = clip_id_list
    else:
        lost_chat_clips = list(
            set(clip_id_list)
            - set([file.split(".")[0] for file in os.listdir(chats_download)])
        )
    lost_chat_df = pd.DataFrame({"clip_id": lost_chat_clips})
    lost_chat_df.to_csv(
        f"{chat_directory}/{user_id}_clips_without_chat_double_check.csv", index=False
    )


def create_report(messaged_re_dir):
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

    # create_report(CHAT_WITH_RE_DIR)


def process_all_users_parallel(
    users_with_chats: list[str], max_workers: int = None
) -> list[Union[dict, None]]:
    """
    Process all users' chat data in parallel using ThreadPoolExecutor

    Args:
        users_with_chats: List of user IDs to process
        max_workers: Maximum number of threads to use (defaults to None, which lets ThreadPoolExecutor decide)

    Returns:
        List of processing results for each user
    """
    all_results = []

    # Initialize ThreadPoolExecutor with specified number of workers
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks and create a future-to-user mapping
        future_to_user = {
            executor.submit(process_chat_csv, user_id): user_id
            for user_id in users_with_chats
        }

        # Process completed tasks as they finish
        for future in as_completed(future_to_user):
            user_id = future_to_user[future]
            try:
                result = future.result()
                all_results.append(result)
                print(f"Completed processing for user {user_id}")
            except Exception as e:
                print(f"Unhandled error processing user {user_id}: {str(e)}")
                all_results.append(
                    {"user_id": user_id, "status": "error", "error": str(e)}
                )

    return all_results


def download_single_video(user_id: str, clip_id: str, output_path: str):
    """
    Download a single video and return the result
    """
    file_path = "data/donload_mp4_fail.csv"
    error_df_columns = ["user_id", "clip_id", "status", "output_path"]
    error_df = read_or_create_csv_file(file_path, error_df_columns)
    try:
        result = subprocess.run(
            [
                "twitch-dl",
                "download",
                clip_id,
                "--output",
                output_path,
                "--quality",
                "source",
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode == 0:
            result_dict = {
                "user_id": user_id,
                "clip_id": clip_id,
                "status": "success",
                "output_path": output_path,
            }
            return result_dict
        else:
            result_dict = {
                "user_id": user_id,
                "clip_id": clip_id,
                "status": "error",
                "error": result.stderr,
                "output_path": output_path,
            }
            concat_df_to_file(
                [error_df, pd.DataFrame([result_dict])], file_path, subset=["clip_id"]
            )
            return result_dict
    except Exception as e:
        result_dict = {
            "user_id": user_id,
            "clip_id": clip_id,
            "status": "error",
            "error": str(e),
            "output_path": output_path,
        }
        concat_df_to_file(
            [error_df, pd.DataFrame([result_dict])], file_path, subset=["clip_id"]
        )
        return result_dict


def download_user_videos(user_id: str):
    """
    Download all videos for a single user
    """
    try:
        user_mp4_directory_path = f"{MP4_DIRECTORY}/{user_id}"
        os.makedirs(user_mp4_directory_path, exist_ok=True)
        user_chat_dir = os.path.join(CHAT_DIRECTORY, str(user_id))

        results = []

        for file in os.listdir(user_chat_dir):
            if file.endswith(".json"):
                clip_id = file.split(".")[0]
                output_path = f"{user_mp4_directory_path}/{clip_id}.mp4"
                # Skip if file already exists
                if os.path.exists(output_path):
                    results.append(
                        {
                            "user_id": user_id,
                            "clip_id": clip_id,
                            "status": "skipped",
                            "message": "File already exists",
                            "output_path": output_path,
                        }
                    )
                    continue
                # Run the subprocess to download the clip to the specified path
                result = download_single_video(user_id, clip_id, output_path)
                results.append(result)
        return {"user_id": user_id, "status": "completed", "results": results}
    except Exception as e:
        return {"user_id": user_id, "status": "error", "error": str(e)}


def download_all_videos_parallel(users_with_chats: list[str], max_workers: int = 4):
    """
    Download videos for all users in parallel using ThreadPoolExecutor

    Args:
        users_with_chats: List of user IDs to process
        max_workers: Maximum number of threads to use (default: 4)

    Returns:
        List of download results for each user
    """
    all_results = []
    total_users = len(users_with_chats)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create a dictionary mapping futures to user_ids
        future_to_user = {
            executor.submit(download_user_videos, user_id): user_id
            for user_id in users_with_chats
        }

        # Use tqdm for progress bar
        with tqdm(total=total_users, desc="Processing users") as pbar:
            for future in as_completed(future_to_user):
                user_id = future_to_user[future]
                try:
                    result = future.result()
                    all_results.append(result)

                    # Log the result
                    if result["status"] == "completed":
                        success_count = sum(
                            1 for r in result["results"] if r["status"] == "success"
                        )
                        total_count = len(result["results"])
                        print(
                            f"User {user_id}: Successfully downloaded {success_count}/{total_count} videos"
                        )
                    else:
                        print(
                            f"User {user_id}: Error - {result.get('error', 'Unknown error')}"
                        )

                except Exception as e:
                    error_msg = f"Unhandled error processing user {user_id}: {str(e)}"
                    print(error_msg)
                    all_results.append(
                        {"user_id": user_id, "status": "error", "error": str(e)}
                    )
                    write_log(DOWNLOAD_MP4_LOG, error_msg)

                pbar.update(1)


if __name__ == "__main__":
    chat_downloader = ChatDownload()
    streamer_names = pd.read_csv("data/users.csv")["display_name"]
    twitch = Twitch(started_at="2025-12-29T00:00:00Z", ended_at="2025-12-30T00:00:00Z")
    retrieve_data_record = f"data/retrieve_{datetime.today().strftime('%Y-%m-%d')}.txt"
    # # Open the file in write mode
    with open(retrieve_data_record, "a") as file:
        # Write some content to the file
        file.write(f"retrieve_data_time: {datetime.now()}\n")
        file.write(f"started_at: {twitch.started_at}\n")
        file.write(f"ended_at: {twitch.ended_at}\n\n")

    user_info_list = []
    user_info = twitch.get_users_by_login_names(streamer_names)
    user_info_list.extend(user_info.get("data", []))
    user_info_df = read_or_create_csv_file(USERS_INFO_FILE)
    if "twitch_user_id" not in user_info_df.columns:
        user_info_df = create_users_info_file(user_info_list, USERS_INFO_FILE)

    # # User without clip record
    user_without_clip_file = f"{CLIP_DIRECTORY}/user_without_clip.csv"
    user_without_clip_df = read_or_create_csv_file(
        user_without_clip_file, columns=["user_id"]
    ).astype(str)

    for user_id in user_info_df["twitch_user_id"][13:14]:
        user_id = str(user_id)
        follower_count = twitch.get_user_follower_count(user_id)
        user_info_df.loc[
            user_info_df["twitch_user_id"] == user_id, "follower_count"
        ] = follower_count
        clip_summary_df = twitch.summary_user_clips_to_csv(user_id)
        if clip_summary_df.empty:
            continue

        clip_urls = dict(
            zip(list(clip_summary_df["clip_id"]), list(clip_summary_df["url"]))
        )
        chat_downloader.download_and_save_chats_from_clips(
            user_id, f"{CHAT_DIRECTORY}/{user_id}", clip_urls
        )

    user_info_df["follower_count"] = user_info_df["follower_count"].apply(
        lambda x: int(x) if pd.notnull(x) else 0
    )
    user_info_df.to_csv(USERS_INFO_FILE, index=False)
    users_with_chats = get_items_in_dir(CHAT_DIRECTORY)
    download_all_videos_parallel(users_with_chats)
    process_all_users_parallel(users_with_chats=users_with_chats, max_workers=20)
