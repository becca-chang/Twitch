#!/bin/bash

# List of streamers (replace with your own list)
streamers=("streamer1" "streamer2" "streamer3")

# Loop through the list of streamers and download their videos
for streamer in "${streamers[@]}"; do
  echo "Downloading videos for $streamer..."
  twitch-dl download "$streamer" --output "/path/to/downloads/$streamer/"
done
