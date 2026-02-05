import re
import json
import argparse


def clean_transcription(text):
    """
    Cleans and normalizes Tibetan transcription text to make it syntactically correct.

    Args:
        text (str): The input transcription text.

    Returns:
        str: The cleaned and normalized transcription text.
    """
    # Replace newline and tab characters with spaces
    text = text.replace('\n', ' ')
    text = text.replace('\t', ' ')
    text = text.strip()
    
    # Normalize specific Tibetan punctuation and characters
    text = re.sub("༌", "་", text)  # Normalize tsak
    text = re.sub("༎", "།", text)  # Normalize double shae
    text = re.sub("༔", "།", text)
    text = re.sub("༏", "།", text)
    text = re.sub("༐", "།", text)
    text = re.sub("ཽ", "ོ", text)  # Normalize
    text = re.sub("ཻ", "ེ", text)  # Normalize

    # Collapse multiple spaces and Tibetan spaces
    text = re.sub(r"\s+།", "།", text)
    text = re.sub(r"།+", "།", text)
    text = re.sub(r"།", "། ", text)
    text = re.sub(r"\s+་", "་", text)
    text = re.sub(r"་+", "་", text)
    text = re.sub(r"\s+", " ", text)

    # Normalize repetitive sequences of Tibetan characters
    text = re.sub(r"ཧཧཧ+", "ཧཧཧ", text)
    text = re.sub(r'ཧི་ཧི་(ཧི་)+', r'ཧི་ཧི་ཧི་', text)
    text = re.sub(r'ཧེ་ཧེ་(ཧེ་)+', r'ཧེ་ཧེ་ཧེ་', text)
    text = re.sub(r'ཧ་ཧ་(ཧ་)+', r'ཧ་ཧ་ཧ་', text)
    text = re.sub(r'ཧོ་ཧོ་(ཧོ་)+', r'ཧོ་ཧོ་ཧོ་', text)
    text = re.sub(r'ཨོ་ཨོ་(ཨོ་)+', r'ཨོ་ཨོ་ཨོ་', text)

    # Remove specific punctuation marks and special characters
    chars_to_ignore_regex = "[\,\?\.\!\-\;\:\"\“\%\‘\”\�\/\{\}\(\)༽》༼《༄༅༈༑༠'|·×༆༸༾ཿ྄྅྆྇ྋ࿒ᨵ​’„╗᩺╚༿᫥ྂ༊ྈ༁༂༃༇༈༉༒༷༺༻࿐࿑࿓࿔࿙࿚༴࿊]"
    text = re.sub(chars_to_ignore_regex, '', text) + " "
    
    return text



def load_config_from_file(config_path):
    """Load configuration from the given JSON file."""
    with open(config_path, 'r') as f:
        config = json.load(f)
    return config


def parse_args_and_load_config():
    """Parse command-line arguments and load the configuration file."""
    parser = argparse.ArgumentParser(description="Run the audio processing pipeline with config.")
    parser.add_argument('--config', type=str, required=True, help='Path to the JSON configuration file.')
    
    args = parser.parse_args()

    # Load configuration from file
    return load_config_from_file(args.config)


def get_time_span(filename):
    """
    Extracts the time span in seconds from a filename.
    
    Args:
        filename (str): The filename from which to extract the time span.
    
    Returns:
        float: The time span in seconds, or 0 if extraction fails.
    """
    filename = filename.lower().replace(".wav", "").replace(".mp3", "")
    try:
        if "_to_" in filename:
            start, end = filename.split("_to_")
        else:
            start, end = filename.split("-")
        start = float(start.split("_")[-1])
        end = float(end.split("_")[0])
        return (end - start) / 1000 if "_to_" in filename else abs(end - start)
    except Exception as err:
        print(f"Error parsing filename '{filename}': {err}")
        return 0