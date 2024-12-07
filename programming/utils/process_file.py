import json


def create_json_file(data, output):
    with open(output, "w") as json_file:
        json.dump(data, json_file, indent=4)


def read_file(file):
    with open(file, "r") as file:  # Opens the file in read mode
        content = json.load(file)
    return content


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
