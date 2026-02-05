import sys
import subprocess

sys.path.append('../util')


from google_utils import read_spreadsheet
from files_utils import collect_segments
from audio_utils import split_audio_files
from common_utils import parse_args_and_load_config
from docx_utils import download_etext

def main(config):
    # Read configuration from the loaded JSON
    from_id = config['FROM_ID']
    to_id = config['TO_ID']
    id_col = config['ID_COL']
    link_col = config['LINK_COL']
    sr_col = config['SR_COL']
    sheet_id = config['SHEET_ID']

    # Read the spreadsheet
    df = read_spreadsheet(sheet_id=sheet_id)

    for index, row in df.iterrows():
        id = row[id_col]
        gd_url = row[link_col]
        sr_no = row[sr_col]

        if sr_no >= from_id and sr_no <= to_id:
            print(id, gd_url)
            print(f"Downloading: {id} from {gd_url}, Sr_no: {sr_no}")
            download_etext(gd_url=gd_url, file_name=id)

   

if __name__ == "__main__":
    # Parse arguments and load config
    config = parse_args_and_load_config()

    # Run the main pipeline logic
    main(config)
