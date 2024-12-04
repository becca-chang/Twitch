import json
import pandas as pd
import re
import whisper
from typing import Optional


def remove_punctuation_from_directory(name: str):
    # Use a regular expression to remove all punctuation
    return re.sub(r"[^\w\s-]", " ", name)


def speech_to_text(speech_file: str, output_file: str):
    model = whisper.load_model("base")
    result = model.transcribe(speech_file)
    print(result)


def make_url(url, repeated_param: str, parameters: list, page: Optional[int] = None):
    url += "?"

    # Loop through the list and add each element as a login parameter
    for param in parameters:
        url += f"{repeated_param}={param}&"

    # Remove the trailing '&' and print the final URL
    url = url.rstrip("&")
    return url


def create_json_file(data, output):
    with open(output, "w") as json_file:
        json.dump(data, json_file, indent=4)


def custom_sort(dict_list, sort_order):
    # Create a mapping of display_name to index in the sort_order list
    order_dict = {name: index for index, name in enumerate(sort_order)}

    # Sort the dictionary list based on the custom order
    return sorted(
        dict_list, key=lambda x: order_dict.get(x["display_name"], len(sort_order))
    )


def read_file(file):
    with open(file, "r") as file:  # Opens the file in read mode
        content = json.load(file)
    return content
