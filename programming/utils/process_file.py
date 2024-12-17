import json
import os
import pandas as pd


def create_json_file(data, output):
    with open(output, "w") as json_file:
        json.dump(data, json_file, indent=4)


def read_or_create_csv_file(file_path, columns=[]) -> pd.DataFrame:

    # Check if the file exists
    if not os.path.exists(file_path):
        # If file does not exist, create an empty DataFrame and save it as CSV
        df = pd.DataFrame(columns=columns)  # Specify your desired columns
        df.to_csv(file_path, index=False)
    else:
        # If file exists, read the data
        df = pd.read_csv(file_path)
    return df


def read_json_file(file_path) -> pd.DataFrame:

    # Check if the file exists
    if not os.path.exists(file_path):
        raise FileExistsError(f"The file '{file_path}' not exists.")
    else:
        # If file exists, read the data
        df = pd.read_json(file_path)
    return df


# Get the list of valid files
def get_files_with_digit_names(directory):
    chat_directory_items = os.listdir(directory)
    valid_files = [
        os.path.join(directory, item)
        for item in chat_directory_items
        if os.path.isfile(os.path.join(directory, item))
        and item.split(".")[0].isdigit()
    ]
    return valid_files


def decode_and_save_json(input_file, output_file=None):
    """
    Decode Unicode in JSON file and save as UTF-8

    Args:
        input_file (str): Path to input JSON file
        output_file (str, optional): Path to save decoded JSON file

    Example usage:
        decoded_data = decode_and_save_json(
            "test_clip_chat_deocde.json", output_file="test_clip_chat_deocde_out.json"
        )
    """
    # Use input filename with .utf8.json extension if no output file specified
    if output_file is None:
        output_file = f"{input_file}.utf8.json"

    try:
        # Read the JSON file
        with open(input_file, "r", encoding="utf-8") as infile:
            data = json.load(infile)

        # Save with UTF-8 encoding
        with open(output_file, "w", encoding="utf-8") as outfile:
            json.dump(data, outfile, ensure_ascii=False, indent=4)

        print(f"File decoded and saved: {output_file}")
        return data

    except Exception as e:
        print(f"Error decoding file: {e}")
        return None
