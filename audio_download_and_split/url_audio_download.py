import os
import sys
import subprocess
from pathlib import Path

sys.path.append('../util')


from google_utils import read_spreadsheet
from files_utils import collect_segments
from audio_utils_silero import split_audio_files
from common_utils import parse_args_and_load_config
from download_utils import download_url_file
from stats_utils import generate_audio_stats_report


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
    
    # Create necessary directories
    Path(download_audio_dir).mkdir(parents=True, exist_ok=True)
    Path(segment_dir).mkdir(parents=True, exist_ok=True)
    print(f"Created directories: {download_audio_dir} and {segment_dir}")

    # Read the spreadsheet
    df = read_spreadsheet(sheet_id=sheet_id)

    for index, row in df.iterrows():
        id = row[id_col]
        url_path = row[link_col]
        sr_no = row[sr_col]

        if sr_no >= from_id and sr_no <= to_id:
            
            audio_filename = f"{download_audio_dir}/{id}.wav".strip()

            print(id, url_path)
            if os.path.exists(audio_filename):
                print(f"Audio file {audio_filename} already exists. Skipping extraction.")
                continue
            if not os.path.exists(audio_filename):
                download_url_file(url_path, audio_filename)

    # Split the audio files
    split_audio_files(prefix, file_format, download_audio_dir, dept)

    # Collect the audio segments
    collect_segments(prefix, f'../data/{dept}_after_split', segment_dir)
    
    # Generate audio statistics report
    stats_output_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), f'data/{dept}_audio_stats.csv')
    print(f"\nGenerating audio statistics report to {stats_output_file}...")
    generate_audio_stats_report(dept, stats_output_file, file_format)

    # Upload the collected segments to the S3 bucket
    subprocess.run(f'aws s3 cp {segment_dir} {s3_bucket} --recursive', shell=True)

if __name__ == "__main__":
    # Parse arguments and load config
    config = parse_args_and_load_config()

    # Run the main pipeline logic
    main(config)
