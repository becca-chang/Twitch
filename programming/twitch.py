import json
import os
import pandas as pd
import requests
import subprocess
import time
import urllib.parse

from typing import Union
from tqdm import tqdm

from chat_downloader import ChatDownloader
from chat_downloader.errors import NoChatReplay
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from utils.utils import *
from utils.process_file import read_or_create_csv_file, read_json_file

CLIENT_ID = "olj1zlf45mtffa1166zd8b1ersrew3"
AUTHORIZATION = "Bearer ao05bvk118sgvfolodd8c585pidnzh"
TWITCH_HEADERS = {"Client-Id": CLIENT_ID, "Authorization": AUTHORIZATION}

DATA_ROOT = "data"
CLIP_DIRECTORY = f"{DATA_ROOT}/clips"
CHAT_DIRECTORY = f"{DATA_ROOT}/chats"
VIDEO_DIRECTORY = f"{DATA_ROOT}/videos"
MP4_DIRECTORY = f"{DATA_ROOT}/mp4"

USERS_INFO_FILE = f"{DATA_ROOT}/users_info.csv"

CHAT_CSV_DIRECTORY = f"{DATA_ROOT}/chats_csv"
CHAT_WITH_RE_DIR = os.path.join(CHAT_CSV_DIRECTORY, "chat_with_re")

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


class TwitchMetric:
    def __init__(self):
        self.driver = webdriver.Chrome()

    def quit(self):
        self.driver.quit()

    def get_top_streamers_by_cat(self, category):
        file_name = f"{remove_punctuation_from_directory(category)}_top_streamers"
        file_path = f"{DATA_ROOT}/{file_name}.csv"
        if os.path.exists(file_path):
            request_logins_list = pd.read_csv(file_path)["login"]
        else:
            streamer_names = []
            rank_list = []
            request_logins_list = []
            for page in [1, 2, 3, 4, 5, 6]:
                url_path = f"https://www.twitchmetrics.net/channels/follower?game={urllib.parse.quote(category)}&lang=en&page={page}"
                self.driver.get(url_path)
                time.sleep(2)
                streamers = self.driver.find_elements(
                    By.CSS_SELECTOR, ".list-group-item h5.mb-0"
                )
                ranks = self.driver.find_elements(
                    By.CSS_SELECTOR, ".list-group-item span.text-muted"
                )
                request_logins = self.driver.find_elements(By.CSS_SELECTOR, ".mb-2 a")

                for rank, streamer, request_login in zip(
                    ranks, streamers, request_logins
                ):
                    streamer_names.append(streamer.text)
                    rank_list.append(rank.text)
                    request_logins_list.append(
                        request_login.get_attribute("href").split("-")[-1]
                    )
            df = pd.DataFrame(
                data={
                    "rank": rank_list,
                    "display_name": streamer_names,
                    "login": request_logins_list,
                }
            )
            df.to_csv(file_path, index=False)
            df.to_csv(USERS_INFO_FILE, index=False)
        return request_logins_list


class Twitch:
    def __init__(
        self, started_at: Optional[str] = None, ended_at: Optional[str] = None
    ):
        self.started_at = started_at
        self.ended_at = ended_at

    def get_users_by_login_names(self, names: list):
        missing_user_file = f"{DATA_ROOT}/missing_users.csv"
        missing_user_df = read_or_create_csv_file(missing_user_file)

        url = make_url("https://api.twitch.tv/helix/users", "login", names)
        payload = {}
        response = requests.request(
            "GET", url, headers=TWITCH_HEADERS, data=payload
        ).json()
        response_display_name = [i["login"] for i in response["data"]]
        missing_user = []
        missing_user = list(set(names) - set(response_display_name))
        new_df = pd.DataFrame(data={"display_name": list(missing_user)})
        concat_df_to_file([new_df], missing_user_file)
        return response, missing_user

    def get_user_follower_count(self, user_id: str):
        url = "https://api.twitch.tv/helix/channels/followers"

        payload = {"broadcaster_id": user_id}
        response = requests.request("GET", url, headers=TWITCH_HEADERS, params=payload)

        return response.json().get("total", 0)

    def get_clip_info(
        self,
        user: str,
        started_at: Optional[str] = None,
        ended_at: Optional[str] = None,
    ):
        """
        Efficiently retrieve clip information with concurrent pagination

        :param user: Twitch user ID
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
                    "broadcaster_id": user,
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
            clip_summary.drop(
                ["thumbnail_url", "embed_url", "vod_offset"],
                axis=1,
                inplace=True,
            )
            clip_summary.rename(columns={"id": "clip_id"}, inplace=True)
            concat_df_to_file(
                [summary_clips, clip_summary], file_path, subset=["clip_id"]
            )
            return clip_summary
        return pd.DataFrame()

    def get_videos_by_ids(self, video_ids):
        total = len(video_ids)
        if total:
            video_info_list = []
            start = 0
            end = 100
            while total > start:
                request_video_ids = video_ids[start:end]
                url = make_url(
                    "https://api.twitch.tv/helix/videos", "id", request_video_ids
                )
                response = requests.request("GET", url, headers=TWITCH_HEADERS, data={})
                r = response.json()
                video_info_list.extend(r.get("data", []))
                start += 100
                end += 100

            return video_info_list
        return None


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
    user_info = read_or_create_csv_file(user_info_file_path)
    df_new = pd.DataFrame(data=data)
    df_new.rename(
        columns={
            "id": "twitch_user_id",
        },
        inplace=True,
    )
    df_new.drop(
        ["type", "profile_image_url", "offline_image_url", "view_count"],
        axis=1,
        inplace=True,
    )
    merged_df = user_info.merge(
        df_new,
        on="login",
    )
    merged_df["twitch_user_id"] = merged_df["twitch_user_id"].astype(str)
    merged_df.to_csv(user_info_file_path, index=False)
    return merged_df


def user_videos_to_csv(video_info_list: list, user_id: str):
    df = pd.DataFrame(data=video_info_list)
    df.rename(
        columns={
            "id": "twitch_video_id",
        },
        inplace=True,
    )
    df.drop(
        [
            "stream_id",
            "user_name",
            "description",
            "published_at",
            "thumbnail_url",
            "viewable",
            "type",
        ],
        axis=1,
        inplace=True,
    )
    df.to_csv(f"{VIDEO_DIRECTORY}/{user_id}.csv", index=False)
    return df


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
        chat_directory (str): chat directory. Defaults to "data/chats".
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
            clip_df = pd.DataFrame(
                data={
                    "author_id": author_id_list,
                    "badges_list": badges_list,
                    "message": messages_list,
                    "message_id": message_ids_list,
                    "time_text": time_texts_list,
                    "time_in_seconds": time_in_seconds_list,
                    "clip_id": clips_id_list,
                    "chats_file_path": chats_file_path_list,
                }
            )
            cleaned_clip_path = f"{CHAT_CSV_DIRECTORY}/{user_id}/{clip_id}.csv"
            clip_df.to_csv(cleaned_clip_path)
            return_dict = {"clip_df": clip_df, "cleaned_clip_path": cleaned_clip_path}
        except Exception as e:
            chat_error_datetime.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            chat_error_user.append(user_id)
            chat_error_file_path.append(origin_file_path)
            chat_error_message.append(e)
            # chats_data.append(file_data)

        # user_all_chats = pd.DataFrame(
        #     data={
        #         "author_id": author_id_list,
        #         "badges_list": badges_list,
        #         "message": messages_list,
        #         "message_id": message_ids_list,
        #         "time_text": time_texts_list,
        #         "time_in_seconds": time_in_seconds_list,
        #         "clip_id": clips_id_list,
        #         "chats_file_path": chats_file_path_list,
        #     }
        # )
        # user_all_chats.to_csv(f"{chat_directory}/{user_id}.csv")

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
def re_message(chat_df, column="message", **kwargs):
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
                clip_info = export_single_user_chats_to_csv(origin_file_path, user_id)

                if clip_info:
                    clip_df = clip_info.get("clip_df")
                    cleaned_clip_path = clip_info.get("cleaned_clip_path")

                    chat_df_with_regex = re_message(
                        clip_df,
                        "message",
                        **{
                            "cheer_pattern": CHEER_PATTERN,
                            "subscribed_pattern": SUBSCRIBED_PATTERN,
                            "gifting_pattern": GIFTING_PATTERN,
                        },
                    )

                    chat_df_with_badge_info = chat_df_with_regex.apply(
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
                write_log("process_chat_csv.txt", message)
        return {"user_id": user_id, "processed_files": results}
    except Exception as e:
        message = f"Error processing user {user_id}: {str(e)}"
        write_log("process_chat_csv.txt", message)
        return {"user_id": user_id, "status": "error", "error": str(e)}


# Define the processing function
# def apply_regex_and_save_to_file(
#     file_full_path, output_directory, cheer_pattern, subscribed_pattern, gifting_pattern
# ):
#     os.makedirs(output_directory, exist_ok=True)
#     try:
#         # Read and process the file
#         df = pd.read_csv(file_full_path)
#         df_new = re_message(
#             df,
#             "message",
#             **{
#                 "cheer_pattern": cheer_pattern,
#                 "subscribed_pattern": subscribed_pattern,
#                 "gifting_pattern": gifting_pattern,
#             },
#         )
#         # Save the processed file
#         output_path = os.path.join(output_directory, os.path.basename(file_full_path))
#         df_new.to_csv(output_path, index=False)
#         print(f"Processed: {file_full_path}")
#     except Exception as e:
#         print(f"Error processing {file_full_path}: {e}")


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
            ['twitch-dl', 'download', clip_id, '--output', output_path, '--quality', 'source'],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            result_dict = {
                "user_id": user_id,
                "clip_id": clip_id,
                "status": "success",
                "output_path": output_path
            }
            return result_dict
        else:
            result_dict = {
                "user_id": user_id,
                "clip_id": clip_id,
                "status": "error",
                "error": result.stderr,
                "output_path": output_path
            }
            concat_df_to_file([error_df, pd.DataFrame([result_dict])], file_path, subset=["clip_id"])
            return result_dict
    except Exception as e:
        result_dict = {
            "user_id": user_id,
            "clip_id": clip_id,
            "status": "error",
            "error": str(e),
            "output_path": output_path
        }
        concat_df_to_file([error_df, pd.DataFrame([result_dict])], file_path, subset=["clip_id"])
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
                    results.append({
                        "user_id": user_id,
                        "clip_id": clip_id,
                        "status": "skipped",
                        "message": "File already exists",
                        "output_path": output_path
                    })
                    continue
                # Run the subprocess to download the clip to the specified path
                result = download_single_video(user_id, clip_id, output_path)
                results.append(result)
        return {
            "user_id": user_id,
            "status": "completed",
            "results": results
        }
    except Exception as e:
        return {
            "user_id": user_id,
            "status": "error",
            "error": str(e)
        }

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
                        success_count = sum(1 for r in result["results"] if r["status"] == "success")
                        total_count = len(result["results"])
                        print(f"User {user_id}: Successfully downloaded {success_count}/{total_count} videos")
                    else:
                        print(f"User {user_id}: Error - {result.get('error', 'Unknown error')}")
                    
                except Exception as e:
                    error_msg = f"Unhandled error processing user {user_id}: {str(e)}"
                    print(error_msg)
                    all_results.append({
                        "user_id": user_id,
                        "status": "error",
                        "error": str(e)
                    })
                    write_log("download_mp4.txt", error_msg)
                
                pbar.update(1)

if __name__ == "__main__":
    # twitch_metric = TwitchMetric()
    # chat_downloader = ChatDownload()
    # category = "Just Chatting"
    # streamer_names = twitch_metric.get_top_streamers_by_cat(category)
    # twitch_metric.quit()
    # twitch = Twitch(started_at="2024-10-01T00:00:00Z", ended_at="2024-12-01T00:00:00Z")
    # retrieve_data_record = f"data/retrieve_{datetime.today().strftime('%Y-%m-%d')}.txt"
    # # Open the file in write mode
    # with open(retrieve_data_record, "a") as file:
    #     # Write some content to the file
    #     file.write(f"retrieve_data_time: {datetime.now()}\n")
    #     file.write(f"started_at: {twitch.started_at}\n")
    #     file.write(f"ended_at: {twitch.ended_at}\n\n")

    # user_index_start, user_index_end = 0, 100
    # user_info_list = []
    # user_info, missing_user = twitch.get_users_by_login_names(
    #     streamer_names[user_index_start:user_index_end]
    # )
    # user_info_list.extend(user_info.get("data", []))
    # while missing_user:
    #     user_index_start = user_index_end
    #     user_index_end += len(missing_user)
    #     user_info, missing_user = twitch.get_users_by_login_names(
    #         streamer_names[user_index_start:user_index_end]
    #     )
    #     user_info_list.extend(user_info.get("data", []))
    # user_info_df = read_or_create_csv_file(USERS_INFO_FILE)
    # if "twitch_user_id" not in user_info_df.columns:
    #     user_info_df = create_users_info_file(user_info_list, USERS_INFO_FILE)

    # # User without clip record
    # user_without_clip_file = f"{CLIP_DIRECTORY}/user_without_clip.csv"
    # user_without_clip_df = read_or_create_csv_file(
    #     user_without_clip_file, columns=["user_id"]
    # ).astype(str)

    # for user_id in user_info_df["twitch_user_id"]:
    #     user_id = str(user_id)
    #     follower_count = twitch.get_user_follower_count(user_id)
    #     user_info_df.loc[
    #         user_info_df["twitch_user_id"] == user_id, "follower_count"
    #     ] = follower_count
    #     clip_summary_df = twitch.summary_user_clips_to_csv(user_id)
    #     if not clip_summary_df.empty:
    #         video_id_list = get_unique_values_from_df_column(
    #             clip_summary_df, "video_id"
    #         )
    #         if not video_id_list:  # User's all clips without video record
    #             user_all_clips_without_video_file = (
    #                 f"{VIDEO_DIRECTORY}/{user_id}_all_clip_without_video.csv"
    #             )
    #             user_all_clips_without_video_file_df = read_or_create_csv_file(
    #                 user_all_clips_without_video_file, ["clip_id"]
    #             )
    #             new_df = pd.DataFrame({"clip_id": clip_summary_df["clip_id"]})
    #             concat_df_to_file(
    #                 [user_all_clips_without_video_file_df, new_df],
    #                 user_all_clips_without_video_file,
    #                 subset=["clip_id"]
    #             )
    #         else:
    #             video_data = twitch.get_videos_by_ids(video_id_list)
    #             if video_data:
    #                 user_videos_to_csv(video_data, user_id)

    #         clip_urls = dict(
    #             zip(list(clip_summary_df["clip_id"]), list(clip_summary_df["url"]))
    #         )
    #         chat_downloader.download_and_save_chats_from_clips(
    #             user_id, f"{CHAT_DIRECTORY}/{user_id}", clip_urls
    #         )
    #         # user_all_chats = export_single_user_chats_to_csv(user_id)
    #         # chat_df_with_regex = re_message(
    #         #     user_all_chats,
    #         #     "message",
    #         #     **{
    #         #         "cheer_pattern": CHEER_PATTERN,
    #         #         "subscribed_pattern": SUBSCRIBED_PATTERN,
    #         #         "gifting_pattern": GIFTING_PATTERN,
    #         #     },
    #         # )
    #         # regex_output_path = os.path.join(CHAT_WITH_RE_DIR, f"{user_id}.csv")
    #         # chat_df_with_regex.to_csv(regex_output_path, index=False)

    #     else:
    #         new_user_without_clip_df = pd.DataFrame(data={"user_id": [user_id]})
    #         concat_df_to_file(
    #             [user_without_clip_df, new_user_without_clip_df], user_without_clip_file
    #         )
    #         continue
    # user_info_df["follower_count"] = user_info_df["follower_count"].apply(
    #     lambda x: int(x) if pd.notnull(x) else 0
    # )
    # user_info_df.to_csv(USERS_INFO_FILE, index=False)
    users_with_chats = get_items_in_dir(CHAT_DIRECTORY)
    download_all_videos_parallel(users_with_chats)
    # process_all_users_parallel(users_with_chats=users_with_chats, max_workers=20)
