import subprocess
import os


def download_mp4(clip_id, output_path):

    # Run the subprocess to download the clip to the specified path
    subprocess.run(
        [
            "twitch-dl",
            "download",
            clip_id,
            "--output",
            output_path,
            "--quality",
            "source",
        ]
    )

    print(f"Clip downloaded to: {output_path}")


download_mp4("TiredCuteTildeDAESuppy-sYcqNraD7D9XMFIx", "data/test.mp4")
