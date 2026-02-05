import sys
import subprocess

sys.path.append('../util')


from google_utils import read_spreadsheet, download_audio_gdrive
from files_utils import collect_segments
from audio_utils import split_audio_files
from common_utils import parse_args_and_load_config

def main(config):
    # Read configuration from the loaded JSON
    dept = config['DEPARTMENT']
    from_id = config['FROM_ID']
    to_id = config['TO_ID']
    id_col = config['ID_COL']
    link_col = config['LINK_COL']
    sr_col = config['SR_COL']
    prefix = config['PREFIX']
    segment_dir = config['SEGMENT_DIR']
    download_audio_dir = config['DOWNLOAD_AUDIO_DIR']
    file_format = config['FILE_FORMAT']
    s3_bucket = config['S3_BUCKET']
    sheet_id = config['SHEET_ID']

    # Read the spreadsheet
    df = read_spreadsheet(sheet_id=sheet_id)

    for index, row in df.iterrows():
        id = row[id_col]
        gd_url = row[link_col]
        sr_no = row[sr_col]

        if sr_no >= from_id and sr_no <= to_id:
            print(id, gd_url)
            id = id + '.wav'
            download_audio_gdrive(gd_url, id, download_audio_dir)



    # Split the audio files
    split_audio_files(prefix, file_format, download_audio_dir)

    # Collect the audio segments
    collect_segments(prefix, 'after_split', segment_dir)

    # Upload the collected segments to the S3 bucket
    subprocess.run(f'aws s3 cp {segment_dir} {s3_bucket} --recursive', shell=True)

if __name__ == "__main__":
    # Parse arguments and load config
    config = parse_args_and_load_config()

    # Run the main pipeline logic
    main(config)
