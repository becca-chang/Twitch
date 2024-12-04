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
from utils import *

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
            data = twitch.get_clip_info(broadcaster_id).get("data")
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


def write_chat_file(chat_url):
    # Create an instance of ChatDownloader
    downloader = ChatDownloader()
    no_chat_replay = []
    try:
        chats = downloader.get_chat(chat_url)
        with open("1201chat.json", "w", encoding="utf-8") as f:
            # # Loop through each message in the chat
            for message in chats:
                # Write the author name and message to the file
                f.write(message)
    except NoChatReplay as e:
        no_chat_replay.append(chat_url)
        print(no_chat_replay)


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


def create_clip_df(data: list, broadcaster_id: str):
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
    existed_df["follower_count"] = df["follower_count"].astype(int)
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
# twitch = Twitch()

# Specify the directory path
clip_directory = "data/clips"

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

# create_video_df


# data = twitch.get_clip_info("494543675").get("data")
# create_clip_df(data, "494543675test")

# url = "https://www.twitch.tv/fanum/clip/HonorableOptimisticLeopardCoolStoryBro-i82vQOJdQgZ8-kHS"
# write_chat_file(url)
