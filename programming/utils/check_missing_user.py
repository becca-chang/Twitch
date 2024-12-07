import pandas as pd

top_streamers = pd.read_csv("Just Chatting_top_streamers.csv")
top_streamers_name = top_streamers["Name"][:100]
user_df = pd.read_csv("data/user_df.csv")
user_df_display = user_df["display_name"]
print(set(top_streamers_name) - set(user_df_display))
