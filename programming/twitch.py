import json
import os
import pandas as pd
import requests
import time
import urllib.parse
from chat_downloader import ChatDownloader
from chat_downloader.errors import NoChatReplay
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from utils.utils import *

CLIENT_ID = "olj1zlf45mtffa1166zd8b1ersrew3"
AUTHORIZATION = "Bearer ao05bvk118sgvfolodd8c585pidnzh"
TWITCH_HEADERS = {"Client-Id": CLIENT_ID, "Authorization": AUTHORIZATION}


class Twitch:
    def __init__(self):
        return
        # self.driver = webdriver.Chrome()

    def quit(self):
        self.driver.quit()

    def get_top_streamers_by_cat(self):
        category = "Just Chatting"
        streamer_names = []
        for page in [1, 2, 3]:
            url_path = f"https://www.twitchmetrics.net/channels/viewership?game={urllib.parse.quote(category)}&lang=en&page={page}"
            self.driver.get(url_path)
            time.sleep(2)
            streamers = self.driver.find_elements(
                By.CSS_SELECTOR, ".list-group-item h5.mb-0"
            )

            for element in streamers:
                streamer_names.append(element.text)
        file_name = f"{remove_punctuation_from_directory(category)}_top_streamers"
        df = pd.DataFrame(data={"Name": streamer_names})
        df.to_csv(f"{file_name}.csv")
        return streamer_names

    def get_users_by_names(self, names: list):
        names = names[:100]
        url = make_url("https://api.twitch.tv/helix/users", "login", names)
        payload = {}
        response = requests.request("GET", url, headers=TWITCH_HEADERS, data=payload)
        r = response.json()
        response_display_name = [i["display_name"] for i in r["data"]]
        missing_user = set(names) - set(response_display_name)
        missing_user_df = pd.DataFrame(data={"display_name": list(missing_user)})
        missing_user_df.to_csv("data/missing_user_df.csv")
        return r

    def get_broadcaster_follower_count(self, broadcaster_id: str):
        url = "https://api.twitch.tv/helix/channels/followers"

        payload = {"broadcaster_id": broadcaster_id}
        params = payload
        response = requests.request("GET", url, headers=TWITCH_HEADERS, params=payload)

        return response.json().get("total", 0)

    def get_clip_info(self, broadcaster_id: str, pagination=0, started_at=None):
        if not started_at:
            started_at = f"{datetime.today().strftime('%Y-%m-%d')}T00:00:00Z"
            started_at = f"2024-10-01T00:00:00Z"
        url = "https://api.twitch.tv/helix/clips"
        result = {"data": []}
        payload = {
            "broadcaster_id": broadcaster_id,
            "started_at": started_at,
        }
        iteration = True
        while iteration:
            if pagination:
                payload["after"] = pagination
            response = requests.request(
                "GET", url, headers=TWITCH_HEADERS, params=payload
            )
            r_data = response.json()
            pagination = r_data.get("pagination").get("cursor")
            result["data"].extend(r_data.get("data", []))
            if not pagination:
                iteration = False
        return result

    def process_clips(self, broadcasters_id: list):
        no_clip_df = pd.read_csv("data/clips/no_clips.csv", index_col=0)
        no_clip_broadcasters_id = []
        for broadcaster_id in broadcasters_id:
            data = self.get_clip_info(broadcaster_id).get("data")
            if data:
                create_clip_df(data, broadcaster_id)
            else:
                no_clip_broadcasters_id.append(broadcaster_id)
        df = pd.DataFrame(data={"broadcaster_id": no_clip_broadcasters_id})
        concated_df = pd.concat([no_clip_df, df], ignore_index=True)
        concated_df.to_csv("data/clips/no_clips.csv")

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

    def write_chat_csv_file(self, user_id: str, clip_urls: dict):
        # Create an instance of ChatDownloader
        no_chat_replay = []
        user_id_dir = f"data/chats/{user_id}"
        for clip_id, clip_url in clip_urls.items():
            try:
                chats = self.downloader.get_chat(clip_url)
                os.makedirs(user_id_dir, exist_ok=True)
                with open(f"{user_id_dir}/{clip_id}.json", "w", encoding="utf-8") as f:
                    json.dump(list(chats), f, ensure_ascii=False, indent=4)
            except NoChatReplay as e:
                continue


def create_user_df(data: json):
    df = pd.DataFrame(data=data["data"])
    df.rename(
        columns={
            "id": "twitch_user_id",
        },
        inplace=True,
    )
    df.drop(
        ["login", "type", "profile_image_url", "offline_image_url", "view_count"],
        axis=1,
        inplace=True,
    )
    df.to_csv("data/user_df.csv")
    return df


def create_video_df(video_info_list: list, user_id: str):
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
    df.to_csv(f"data/videos/{user_id}.csv")
    return df


def create_clip_df(data, broadcaster_id: str):
    df = pd.DataFrame(data=data)
    df.drop(
        ["thumbnail_url", "embed_url", "vod_offset"],
        axis=1,
        inplace=True,
    )
    df.to_csv(f"data/clips/{broadcaster_id}.csv")
    return df


def get_followers_count(twitch_instance, broadcasters: list):
    follower_dict = {}
    for id in broadcasters:
        count = twitch_instance.get_broadcaster_follower_count(id)
        follower_dict[id] = count
    return follower_dict


def add_follower_count_into_existed_df(follower_dict, existed_df):
    for id, count in follower_dict.items():
        existed_df.loc[existed_df["twitch_user_id"] == id, "follower_count"] = count
    existed_df["follower_count"] = existed_df["follower_count"].astype(int)
    existed_df.to_csv("data/user_df.csv")


def get_unique_video_ids_from_df(df) -> list:
    df_clean = df[df["video_id"].notna()]
    df_clean["video_id"] = df_clean["video_id"].astype(int, errors="ignore")
    videos = list(set(df_clean["video_id"]))
    return videos


twitch = Twitch()
# streamers = twitch.get_top_streamers_by_cat()
# twitch.quit()
# r = twitch.get_users_by_names(streamers)
# df = create_user_df(r)
# broadcasters_id = df["twitch_user_id"]
# broadcasters_name = df["display_name"]
# follower_dict = get_followers_count(twitch, broadcasters_id)
# add_follower_count_into_existed_df(follower_dict, df)
# user_df = pd.read_csv("data/user_df.csv")
# broadcasters_id = user_df["twitch_user_id"]
# twitch.process_clips(broadcasters_id)

# Specify the directory path
clip_directory = "data/clips"


def process_videos(twitch, clip_directory):
    # Iterate over each item in the directory
    for file in os.listdir(clip_directory):
        if "no_clips" not in file:
            full_path = os.path.join(clip_directory, file)
            df = pd.read_csv(full_path)
            video_ids = get_unique_video_ids_from_df(df)
            data = twitch.get_videos_by_ids(video_ids)
            if data:
                create_video_df(data, file.split(".")[0])
            else:
                with open("data/videos/user_has_no_video.txt", "a") as no_video_record:
                    no_video_record.write(f"{file}\n")


# process_videos(twitch, clip_directory)

chatdownloader = ChatDownload()


def process_chats(chatdownloader):
    error_path = []
    error_string = []
    for file in os.listdir(clip_directory):
        chats_dict = {}
        if "no_clips" not in file:
            full_path = os.path.join(clip_directory, file)
            try:
                df = pd.read_csv(full_path)
            except Exception as e:
                error_path.append(full_path)
                error_string.append(e)
                continue
            result_dict = dict(zip(df["id"], df["url"]))
            chatdownloader.write_chat_csv_file(file.split(".")[0], result_dict)
    error_df = pd.DataFrame(
        data={"errop_path": error_path, "error_string": error_string}
    )
    error_df.to_csv("data/clips/read_csv_fail.csv")
    # print(result_dict)


# process_chats(chatdownloader)
chat_directory = "data/chats"


def chats_to_df(chat_directory):
    chat_error_file = f"{chat_directory}/chats_to_df_errors.csv"
    chat_empty_file = f"{chat_directory}/chats_to_df_empty.csv"
    chat_error_file_path = []
    chat_error_message = []
    empty = {
        "user_id": [],
        "file": [],
    }
    for item in os.listdir(chat_directory):
        item_path = os.path.join(chat_directory, item)  # "data/chats/<user_id>"
        if os.path.isdir(item_path):  # chat_path_base_on_user
            user_id = item
            author_id_list = []
            messages_list = []
            message_ids_list = []
            time_texts_list = []
            time_in_seconds_list = []
            clips_id_list = []
            chats_file_path_list = []
            for file in os.listdir(item_path):  # "data/chats/<user_id>/<clip_id>.json"
                chat_file = os.path.join(
                    item_path, file
                )  # 'data/chats/100869214/MildBlindingEelFloof-RnekrluTMQ3PlSfh.json'
                try:
                    df_chat = pd.read_json(chat_file)
                    if df_chat.empty:
                        empty.get("user_id").append(user_id)
                        empty.get("file").append(chat_file)
                        continue
                    df_chat["author"]
                    # author
                    author_id = [i.get("id") for i in df_chat["author"]]
                    author_id_list.extend(author_id)
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
                    clip_id = [file.split(".")[0] for _ in range(len(df_chat))]
                    clips_id_list.extend(clip_id)
                    # chat file path
                    chats_file = [chat_file for _ in range(len(df_chat))]
                    chats_file_path_list.extend(chats_file)
                except Exception as e:
                    chat_error_file_path.append(chat_file)
                    chat_error_message.append(e)
                # chats_data.append(file_data)

            user_all_chats = pd.DataFrame(
                data={
                    "author_id": author_id_list,
                    "message": messages_list,
                    "message_id": message_ids_list,
                    "time_text": time_texts_list,
                    "time_in_seconds": time_in_seconds_list,
                    "clip_id": clips_id_list,
                    "chats_file_path": chats_file_path_list,
                }
            )
            user_all_chats.to_csv(f"{chat_directory}/{user_id}.csv")
    errors_df = pd.DataFrame(
        {
            "chat_error_file_path": chat_error_file_path,
            "chat_error_message": chat_error_message,
        }
    )
    errors_df.to_csv(chat_error_file)
    empty_df = pd.DataFrame(empty)
    empty_df.to_csv(chat_empty_file)


def chats_to_df_given_user(chat_directory, user: str):
    chat_error_file = f"{chat_directory}/chats_to_df_errors.csv"
    chat_empty_file = f"{chat_directory}/chats_to_df_empty.csv"
    chat_error_df = pd.read_csv(chat_error_file, index_col=0)
    chat_empty_df = pd.read_csv(chat_empty_file, index_col=0)
    chat_error_file_path = []
    chat_error_message = []
    empty = {
        "user_id": [],
        "file": [],
    }
    user_dir = os.path.join(chat_directory, user)
    if os.path.exists(user_dir):  # "data/chats/<user_id>"
        user_id = user
        author_id_list = []
        messages_list = []
        message_ids_list = []
        time_texts_list = []
        time_in_seconds_list = []
        clips_id_list = []
        chats_file_path_list = []
        for file in os.listdir(user_dir):  # "data/chats/<user_id>/<clip_id>.json"
            chat_file = os.path.join(
                user_dir, file
            )  # 'data/chats/100869214/MildBlindingEelFloof-RnekrluTMQ3PlSfh.json'
            try:
                df_chat = pd.read_json(chat_file)
                if df_chat.empty:
                    empty.get("user_id").append(user_id)
                    empty.get("file").append(chat_file)
                    continue
                df_chat["author"]
                # author
                author_id = [i.get("id") for i in df_chat["author"]]
                author_id_list.extend(author_id)
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
                clip_id = [file.split(".")[0] for _ in range(len(df_chat))]
                clips_id_list.extend(clip_id)
                # chat file path
                chats_file = [chat_file for _ in range(len(df_chat))]
                chats_file_path_list.extend(chats_file)
            except Exception as e:
                chat_error_file_path.append(chat_file)
                chat_error_message.append(e)
            # chats_data.append(file_data)

        user_all_chats = pd.DataFrame(
            data={
                "author_id": author_id_list,
                "message": messages_list,
                "message_id": message_ids_list,
                "time_text": time_texts_list,
                "time_in_seconds": time_in_seconds_list,
                "clip_id": clips_id_list,
                "chats_file_path": chats_file_path_list,
            }
        )
        user_all_chats.to_csv(f"{chat_directory}/{user_id}.csv")

        errors_df = pd.DataFrame(
            {
                "chat_error_file_path": chat_error_file_path,
                "chat_error_message": chat_error_message,
            }
        )
        empty_df = pd.DataFrame(empty)
        errors_df = pd.concat([chat_error_df, errors_df], ignore_index=True)
        errors_df.to_csv(chat_error_file)
        empty_df = pd.concat([chat_empty_df, empty_df], ignore_index=True)
        empty_df.to_csv(chat_empty_file)
        return user_dir
    else:  # dir not exists
        return None


# chats_to_df(chat_directory)


# Regular expression message
def re_message(df, column, **kwargs):
    df["subscribed_type"] = None
    df["cheer"] = None
    df["tier_level"] = None
    df["subscribed_month"] = None
    df["gifting_count"] = None
    df["re_message_error"] = None
    cheer_pattern = kwargs.get("cheer_pattern")
    subscribed_pattern = kwargs.get("subscribed_pattern")
    gifting_pattern = kwargs.get("gifting_pattern")
    messages = list(df[column].astype(str))
    for index, message in enumerate(messages):
        try:
            if re.match(cheer_pattern, message):  # 小奇點
                df.loc[index, "subscribed_type"] = 3
                df.loc[index, "cheer"] = re.match(cheer_pattern, message).group(1)
            elif re.search(subscribed_pattern, message):  # 自己訂閱
                df.loc[index, "subscribed_type"] = 1
                df.loc[index, "tier_level"] = re.search(
                    subscribed_pattern, message
                ).group(1)
                df.loc[index, "subscribed_month"] = re.search(
                    subscribed_pattern, message
                ).group(2)
            elif re.search(gifting_pattern, message):  # 贈送訂閱
                df.loc[index, "subscribed_type"] = 2
                df.loc[index, "tier_level"] = re.search(gifting_pattern, message).group(
                    2
                )
                df.loc[index, "gifting_count"] = re.search(
                    gifting_pattern, message
                ).group(1)
            else:
                df.loc[index, "subscribed_type"] = 0
        except Exception as e:
            df.loc[index, "re_message_error"] = e
            continue
    return df


# Define the processing function
def process_file(
    file_full_path, output_directory, cheer_pattern, subscribed_pattern, gifting_pattern
):
    try:
        # Read and process the file
        df = pd.read_csv(file_full_path)
        df_new = re_message(
            df,
            "message",
            **{
                "cheer_pattern": cheer_pattern,
                "subscribed_pattern": subscribed_pattern,
                "gifting_pattern": gifting_pattern,
            },
        )
        # Save the processed file
        output_path = os.path.join(output_directory, os.path.basename(file_full_path))
        df_new.to_csv(output_path, index=False)
        print(f"Processed: {file_full_path}")
    except Exception as e:
        print(f"Error processing {file_full_path}: {e}")


# Prepare the output directory
# output_directory = os.path.join(chat_directory, "messaged_re")
# os.makedirs(output_directory, exist_ok=True)


# Get the list of valid files
def get_valid_files(chat_directory):
    chat_directory_items = os.listdir(chat_directory)
    valid_files = [
        os.path.join(chat_directory, item)
        for item in chat_directory_items
        if os.path.isfile(os.path.join(chat_directory, item))
        and item.split(".")[0].isdigit()
    ]
    return valid_files


cheer_pattern = r"Cheer(\d+)(?:\s|$)"
subscribed_pattern = r"subscribed at Tier (\d+).*?(\d+|\w+) month"
gifting_pattern = r"gifting (\d+) Tier (\d+) Subs to (\w+)'s community"

# valid_files = get_valid_files(chat_directory)
# Use ThreadPoolExecutor for threading
# with ThreadPoolExecutor(max_workers=200) as executor:
#     for file_path in valid_files:
#         executor.submit(
#             process_file,
#             file_path,
#             output_directory,
#             cheer_pattern,
#             subscribed_pattern,
#             gifting_pattern,
#         )

clip_directory = "data/clips"
chat_directory = "data/chats"


def get_clips_without_chats(clip_directory, chat_directory):
    user_clips = {}
    for file in os.listdir(clip_directory):
        user_id = file.split(".")[0]
        if ("no_clips" not in file) and (file.split(".")[0].isdigit()):
            full_path = os.path.join(clip_directory, file)  # user's clips
            df = pd.read_csv(full_path)  # user's clips
            clip_id_list = df["id"]
            user_clips[user_id] = clip_id_list
    for user, clip_id_list in user_clips.items():
        chats_download = f"{chat_directory}/{user}"
        if not os.path.exists(chats_download):
            lost_chat_clips = clip_id_list
        else:
            lost_chat_clips = list(
                set(clip_id_list)
                - set([file.split(".")[0] for file in os.listdir(chats_download)])
            )
        lost_chat_df = pd.DataFrame({"clip_id": lost_chat_clips})
        lost_chat_df.to_csv(f"{chat_directory}/{user}_clips_has_no_chat.csv")


def get_clips_without_chats_given_users(
    clip_directory: str, chat_directory: str, given_users: list
):
    user_clips = {}
    for user_id in given_users:
        full_path = os.path.join(clip_directory, f"{user_id}.csv")  # user's clips
        df = pd.read_csv(full_path)  # user's clips
        clip_id_list = df["id"]
        user_clips[user_id] = clip_id_list

    for user, clip_id_list in user_clips.items():
        chats_download = f"{chat_directory}/{user}"
        if not os.path.exists(chats_download):
            lost_chat_clips = clip_id_list
        else:
            lost_chat_clips = list(
                set(clip_id_list)
                - set([file.split(".")[0] for file in os.listdir(chats_download)])
            )
        lost_chat_df = pd.DataFrame({"clip_id": lost_chat_clips})
        lost_chat_df.to_csv(f"{chat_directory}/{user}_clips_has_no_chat.csv")


# get_clips_without_chats(clip_directory, chat_directory)


def create_report(messaged_re_dir):
    user_reports_list = []
    for file in os.listdir(messaged_re_dir):
        user_id = file.split(".")[0]
        if user_id.isdigit():
            full_file_path = os.path.join(messaged_re_dir, file)
            df = pd.read_csv(full_file_path, index_col=0)

            message_count = df["message_id"].count()
            distinct_clip_count = df["clip_id"].nunique()
            subscribed_count = df[df["subscribed_type"] == 1]["tier_level"].count()
            gifting_count = df[df["subscribed_type"] == 2]["gifting_count"].count()
            gifting_amount = int(df[df["subscribed_type"] == 2]["gifting_count"].sum())
            cheer_count = df[df["subscribed_type"] == 3]["message"].count()
            cheer_amount = int(df[df["subscribed_type"] == 3]["cheer"].sum())
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


messaged_re_dir = "data/chats/messaged_re"
create_report(messaged_re_dir)


# Make up data
def make_up_missing_into_user_df(twitch, users):

    user_df = pd.read_csv("data/make_up_missing_user.csv", index_col=0)
    r = twitch.get_users_by_names(users)
    follower_count_list = []
    twitch_user_id_list = []
    display_name_list = []
    broadcaster_type_list = []
    created_at_list = []
    description_list = []
    for user in r.get("data"):
        twitch_user_id = user["id"]
        twitch_user_id_list.append(twitch_user_id)
        display_name_list.append(user["display_name"])
        broadcaster_type_list.append(user["broadcaster_type"])
        created_at_list.append(user["created_at"])
        description_list.append(user["description"])
        follower_count_list.append(
            twitch.get_broadcaster_follower_count(twitch_user_id)
        )
    new_df = pd.DataFrame(
        {
            "twitch_user_id": twitch_user_id_list,
            "display_name": display_name_list,
            "broadcaster_type": broadcaster_type_list,
            "created_at": created_at_list,
            "description": description_list,
            "follower_count": follower_count_list,
        }
    )
    concated_df = pd.concat([user_df, new_df], ignore_index=True)
    concated_df.to_csv("data/make_up_missing_user.csv")


# make_up_missing_into_user_df(
#     twitch,
#     ["officedrummer"],
# )
users_missing_clips = [
    "59016177",
    "61335991",
    "82350088",
    "801798086",
    "496970086",
    "481029760",
]
users_missing_clips = ["742288302"]
# output_directory = os.path.join(chat_directory, "messaged_re")
# twitch.process_clips(users_missing_clips)
# users_no_clips = list(
#     pd.read_csv("data/clips/no_clips.csv", index_col=0)["broadcaster_id"].astype(str)
# )
# users_still_missing_clips = []
# for user_id in users_missing_clips:
#     if user_id in users_no_clips:
#         users_still_missing_clips.append(user_id)
#     else:
#         clip_df = pd.read_csv(f"data/clips/{user_id}.csv")
#         video_ids = get_unique_video_ids_from_df(clip_df)
#         data = twitch.get_videos_by_ids(video_ids)
#         if data:
#             create_video_df(data, user_id)
#         else:
#             with open("data/videos/user_has_no_video.txt", "a") as no_video_record:
#                 no_video_record.write(f"\n{user_id}.csv")
#         df = pd.read_csv(f"data/clips/{user_id}.csv")
#         result_dict = dict(zip(df["id"], df["url"]))
#         chatdownloader.write_chat_csv_file(user_id, result_dict)
#         if chats_to_df_given_user(chat_directory, user_id):
#             df = pd.read_csv(f"{chat_directory}/{user_id}.csv", index_col=0)
#             df_new = re_message(
#                 df,
#                 "message",
#                 **{
#                     "cheer_pattern": cheer_pattern,
#                     "subscribed_pattern": subscribed_pattern,
#                     "gifting_pattern": gifting_pattern,
#                 },
#             )
#             output_path = os.path.join(output_directory, f"{user_id}.csv")
#             df_new.to_csv(output_path, index=False)

# print(users_still_missing_clips)
# get_clips_without_chats_given_users(
#     clip_directory,
#     chat_directory,
#     list(set(users_missing_clips) - set(users_still_missing_clips)),
# )
