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
    existing_files_df = pd.read_csv(f'../data/{dept}.csv')  # Replace with your actual CSV file name with all inference

    # List to store matching rows
    matching_rows = []

    for index, row in df.iterrows():
        id = row[id_col]
        sr_no = row[sr_col]

        if sr_no >= from_id and sr_no <= to_id:
            print(id, sr_no)
            matching_rows.extend(existing_files_df[existing_files_df['file_name'].str.startswith(id)].to_dict('records'))

    # Convert the matching rows to a DataFrame
    matching_rows_df = pd.DataFrame(matching_rows)
    # Drop any "Unnamed" columns from the DataFrame
    matching_rows_df = matching_rows_df.loc[:, ~matching_rows_df.columns.str.contains('^Unnamed')]
    # Save the matching rows to 'pc.csv' without including the index
    matching_rows_df.head()
    # Save the matching rows  without including the index
    matching_rows_df.to_csv(f"../data/{dept}_{from_id}_to_{to_id}.csv", index=False)

    group_id = group_id
    last_db_id = get_max_db_id()
    
    matching_rows_df['group_id'] = group_id
    matching_rows_df['state'] = 'transcribing'
    matching_rows_df['id'] = matching_rows_df.index + last_db_id + 1
    
    matching_rows_df.to_csv(f"../data/{dept}_{group_id}_{from_id}_to_{to_id}.csv", index=False)
    
if __name__ == "__main__":
    # Parse arguments and load config
    config = parse_args_and_load_config()

    # Run the main pipeline logic
    main(config)
