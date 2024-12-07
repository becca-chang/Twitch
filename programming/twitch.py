import pandas as pd
import os
import requests
import time
import urllib.parse
from chat_downloader import ChatDownloader
from chat_downloader.errors import NoChatReplay
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

    def process_clips(self, broadcasters_id):
        no_clip_broadcasters_id = []
        for broadcaster_id in broadcasters_id:
            data = self.get_clip_info(broadcaster_id).get("data")
            if data:
                create_clip_df(data, broadcaster_id)
            else:
                no_clip_broadcasters_id.append(broadcaster_id)
        no_clip_df = pd.DataFrame(data={"broadcaster_id": no_clip_broadcasters_id})
        no_clip_df.to_csv("data/clips/no_clips.csv")

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
        for chat_id, chat_url in clip_urls.items():
            try:
                chats = self.downloader.get_chat(chat_url)
                os.makedirs(user_id_dir, exist_ok=True)
                with open(f"{user_id_dir}/{chat_id}.json", "w", encoding="utf-8") as f:
                    json.dump(list(chats), f, ensure_ascii=False, indent=4)
            except NoChatReplay as e:
                no_chat_replay.append(chat_url)
        df = pd.DataFrame(data={"user_id": user_id, "chat_url": no_chat_replay})
        df.to_csv("data/chats/no_chat_replay.csv")


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


def get_unique_video_ids_from_df(df):
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
    chat_error_file = f"{chat_directory}/errors.csv"
    chat_empty_file = f"{chat_directory}/empty.csv"
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
            for file in os.listdir(item_path):  # "data/chats/<user_id>/<clip_id>"
                chat_file = os.path.join(item_path, file)
                try:
                    df_chat = pd.read_json(chat_file)
                    if df_chat.empty:
                        empty.get("user_id").append(user_id)
                        empty.get("file").append(item_path)
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
                    clip_id = [file for _ in range(len(df_chat))]
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


# chats_to_df(chat_directory)


# Make up data
def make_up_missing_into_user_df(twitch, users):
    user_df = pd.read_csv("data/user_df.csv")
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


# make_up_missing_into_user_df(twitch, ["Tray"])
# result = twitch.get_clip_info("103314254")
# create_clip_df(result.get("data"), "103314254")
