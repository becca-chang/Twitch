import subprocess
import os
def download_mp4(clip_id, output_path):

    # Run the subprocess to download the clip to the specified path
    subprocess.run(['twitch-dl', 'download', clip_id, '--output', output_path, '--quality', 'source'])

    print(f"Clip downloaded to: {output_path}")

# Define the directory you want to list
directory_path = 'data/chats'

# List all files and subdirectories in the specified directory
entries = os.listdir(directory_path)

# If you want to filter for directories specifically
directories = [entry for entry in entries if os.path.isdir(os.path.join(directory_path, entry))]

print(directories)

for stramer_dir in directories:
    directory = f"data/mp4/{stramer_dir}"
    print(directory)
    os.makedirs(directory, exist_ok=True)
    files =[]
    for f in os.listdir(os.path.join(directory_path, stramer_dir)):
        if f.endswith(".json"):
            clip_id = f.split(".")[0]
            output_path = f"{directory}/{clip_id}.mp4"
            download_mp4(clip_id, output_path)
