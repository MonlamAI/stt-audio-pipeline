import os
import sys
import subprocess

sys.path.append('../util')

from google_utils import read_spreadsheet
from db_utils import get_max_db_id
from tqdm.auto import tqdm
import pandas as pd
from pathlib import Path
from common_utils import parse_args_and_load_config
from docx_utils import transfer_text
from evaluate import load

# Load the CER metric from the "evaluate" library
cer_metric = load("cer")

def calculate_cer(reference, prediction):
    """Calculate the Character Error Rate (CER) using the evaluate library."""
    try:
        cer = cer_metric.compute(references=[reference], predictions=[prediction])
        return min(cer, 1.0)  # Ensure CER does not exceed 1.0
    except Exception as e:
        print(f"Error calculating CER: {e}")
        return 1.0  # Return a high CER for safety


def main(config):
    # Read configuration from the loaded JSON
    dept = config['DEPARTMENT']
    from_id = config['FROM_ID']
    to_id = config['TO_ID']
    id_col = config['ID_COL']
    sr_col = config['SR_COL']
    segment_dir = config['SEGMENT_DIR']
    sheet_id = config['SHEET_ID']
    group_id = config['GROUP_ID']

    # Read the spreadsheet
    df = read_spreadsheet(sheet_id=sheet_id)

    # Read the CSV file with existing file names
    existing_files_df = pd.read_csv(f'../data/{dept}.csv')  # Replace with your actual CSV file name

    # List to store matching rows
    matching_rows = []

    for index, row in df.iterrows():
        id = row[id_col]
        sr_no = row[sr_col]

        if sr_no >= from_id and sr_no <= to_id:
            print(id, sr_no)
            matching_rows.extend(existing_files_df[existing_files_df['file_name'].str.startswith(f"{id}_")].to_dict('records'))

    # Convert the matching rows to a DataFrame
    matching_rows_df = pd.DataFrame(matching_rows)
    # Drop any "Unnamed" columns from the DataFrame
    matching_rows_df = matching_rows_df.loc[:, ~matching_rows_df.columns.str.contains('^Unnamed')]
    # Save the matching rows to 'pc.csv' without including the index
    matching_rows_df.head()
    # Save the matching rows  without including the index
    matching_rows_df.to_csv(f"../data/{dept}_{from_id}_to_{to_id}.tsv", index=False, sep="\t")

    temp = []

    for index, row in df.iterrows():
        id = row[id_col]
        sr_no = row[sr_col]

        if sr_no >= from_id and sr_no <= to_id:
            print(id, sr_no)
            transfer_text_df, status = transfer_text(f'../util/etexts/{id}.txt', f"../data/{dept}_{from_id}_to_{to_id}.tsv", id)
            temp.append(transfer_text_df)
            print(status)

    transfered_text_df = pd.concat(temp)  
    transfered_text_df.head()

    transfered_text_df.fillna('', inplace=True)
    transfered_text_df = transfered_text_df[transfered_text_df['inference_transcript'].apply(lambda x: len(x) < 500)]
    transfered_text_df = transfered_text_df.sort_values('file_name')
    transfered_text_df = transfered_text_df.reset_index(drop=True)
    
    # Adjust inference_transcripts based on CER
    total_cer = 0
    cer_count = 0

    for index, row in transfered_text_df.iterrows():
        file_name = row['file_name']
        transferred_inference_transcript = row['inference_transcript']

        # Get the corresponding row from the original CSV
        matching_row = matching_rows_df[matching_rows_df['file_name'] == file_name]
        if not matching_row.empty:
            original_inference_transcript = matching_row.iloc[0]['inference_transcript']

            # Calculate CER
            cer_value = calculate_cer(original_inference_transcript, transferred_inference_transcript)
            total_cer += cer_value
            cer_count += 1

            print(f"CER for file {file_name}: {cer_value}")

    # Calculate average CER
    avg_cer = total_cer / cer_count if cer_count > 0 else 0
    print(f"Average CER: {avg_cer}")

    # If average CER > 0.4, replace all inference transcripts with original ones
    if avg_cer > 0.4:
        print("Average CER exceeds 0.4. Replacing all inference transcripts with original ones.")
        for index, row in transfered_text_df.iterrows():
            file_name = row['file_name']
            matching_row = matching_rows_df[matching_rows_df['file_name'] == file_name]
            if not matching_row.empty:
                original_inference_transcript = matching_row.iloc[0]['inference_transcript']
                transfered_text_df.at[index, 'inference_transcript'] = original_inference_transcript
    # Compare the 'file_name' columns to find missing entries
    missing_file_names_df = matching_rows_df[~matching_rows_df['file_name'].isin(transfered_text_df['file_name'])]

    df_combined = pd.concat([missing_file_names_df, transfered_text_df], ignore_index=True)
    # Update missing inference_transcript
    df_combined['inference_transcript'] = df_combined['inference_transcript'].fillna('')
    df_combined['inference_transcript'] = df_combined.apply(
        lambda row: row['inference_transcript']
        if row['inference_transcript'].strip() != ''
        else matching_rows_df.loc[
            matching_rows_df['file_name'] == row['file_name'], 'inference_transcript'
        ].values[0],
        axis=1
    )
    # Save the result to a new CSV file or display it
    group_id = group_id
    last_db_id = get_max_db_id()

    df_combined['group_id'] = group_id
    df_combined['state'] = 'transcribing'
    df_combined['id'] = matching_rows_df.index + last_db_id + 1

    df_combined.to_csv(f"../data/{dept}_{group_id}_{from_id}_to_{to_id}_transfered.csv", index=False)
    
    
if __name__ == "__main__":
    # Parse arguments and load config
    config = parse_args_and_load_config()

    # Run the main pipeline logic
    main(config)
