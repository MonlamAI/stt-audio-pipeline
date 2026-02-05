import os
import pandas as pd
from tqdm.auto import tqdm
from anthropic import Client
import sys

sys.path.append('../util')

from common_utils import parse_args_and_load_config

# Initialize Claude AI client
API_KEY = os.getenv('ANTHROPIC_API_KEY')
client = Client(api_key=API_KEY)
def correct_transcription(inference_text: str, transfer_text: str) -> str:
    """
    Corrects the inference transcription by comparing it with the transferred text using Claude AI.

    Args:
        inference_text (str): The initial transcription text that may contain spelling errors.
        transfer_text (str): The transferred text with accurate spelling 

    Returns:
        str: The corrected transcription with spelling errors fixed and context preserved.
    """
    
    prompt = f"""
    \n\nHuman: Here is an initial transcription with context accuracy:
    {inference_text}

    Here is the transferred text with accurate spelling:
    {transfer_text}

    Please review the initial transcription and correct **only the spelling errors** by referring to the words in the transferred text. 
    - Ensure that the overall structure and context of the initial transcription remain unchanged.
    - If any word in the initial transcription is misspelled and a correct spelling is found in the transferred text, use it as a correction.
    - If a word in the initial transcription is correct or if no similar match exists in the transferred text, leave it unchanged.

    Output only the corrected transcription without any additional explanation, ensuring the meaning and structure remain the same.
    \n\nAssistant:
    """
    response = client.completions.create(
        prompt=prompt,
        stop_sequences=["\n\nHuman:"],
        model="claude-2",
        max_tokens_to_sample=500,
        temperature=0.7
    )

    # Clean response to remove extra text like "Here is the corrected transcription:"
    corrected_text = response.completion.strip()

    # Remove any leading explanation in English if present
    if "Here is the corrected transcription:" in corrected_text:
        corrected_text = corrected_text.split("Here is the corrected transcription:")[1].strip()

    return corrected_text

def main(config):
    # Extract configuration
    dept = config['DEPARTMENT']
    from_id = config['FROM_ID']
    to_id = config['TO_ID']
    group_id = config['GROUP_ID']

    # Load inference CSV
    inference_csv_path = f'../data/{dept}.csv'
    transfer_csv_path = f'../data/{dept}_{from_id}_to_{to_id}_transferred.csv'
    output_csv_path = f'../data/{dept}_{group_id}_{from_id}_to_{to_id}_corrected.csv'

    print("Reading inference CSV...")
    if not os.path.exists(inference_csv_path):
        raise FileNotFoundError(f"Inference CSV file not found: {inference_csv_path}")
    inference_csv_files_df = pd.read_csv(inference_csv_path)
    inference_csv_files_df['inference_transcript'] = inference_csv_files_df['inference_transcript'].astype(str).str.strip()

    print("Reading transferred text CSV...")
    if not os.path.exists(transfer_csv_path):
        raise FileNotFoundError(f"Transferred Text CSV file not found: {transfer_csv_path}")
    existing_transfer_csv_files_df = pd.read_csv(transfer_csv_path)
    existing_transfer_csv_files_df['inference_transcript'] = existing_transfer_csv_files_df['inference_transcript'].astype(str).str.strip()

    # Merge the datasets
    print("Merging inference and transfer data...")
    merged_data = pd.merge(
        inference_csv_files_df,
        existing_transfer_csv_files_df[['file_name', 'inference_transcript']],
        on='file_name',
        suffixes=('_inference', '_transfer')
    )

    corrected_transcriptions = []
    is_changed_flags = []

    print("Processing corrections...")
    for _, row in tqdm(merged_data.iterrows(), total=len(merged_data), desc="Correcting Transcriptions"):
        file_name = row['file_name']
        original_inference_text = row['inference_transcript_inference']
        transfer_text = row['inference_transcript_transfer']

        # Correct transcription
        corrected_text = correct_transcription(original_inference_text, transfer_text)

        # Track changes
        is_changed_flags.append(corrected_text != original_inference_text)
        corrected_transcriptions.append(corrected_text)

    # Update the inference DataFrame with corrections
    inference_csv_files_df['inference_transcript'] = corrected_transcriptions  # Replace original column
    inference_csv_files_df['is_changed'] = is_changed_flags

    # Save the corrected CSV
    inference_csv_files_df.to_csv(output_csv_path, index=False)
    print(f"Corrected transcriptions saved to {output_csv_path}")

if __name__ == "__main__":
    # Parse arguments and load config
    config = parse_args_and_load_config()

    # Run the main pipeline logic
    main(config)
