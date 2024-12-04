from moviepy import *

# Load the MKV file
input_file = "2024-01-08_2026556023_tso_sage_what_if.mkv"
output_file = "output_video.mp4"

# Load the video and write it to a new file in MP4 format
video = VideoFileClip(input_file)
video.write_videofile(output_file, codec='libx264')
