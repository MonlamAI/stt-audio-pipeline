import os
import sys
import subprocess

sys.path.append('../util')


from google_utils import read_spreadsheet
from files_utils import collect_segments
from audio_utils import split_audio_files, convert_all_to_16K, batch_convert_mp3_to_wav, extract_audio
from common_utils import parse_args_and_load_config
from download_utils import download_url_file

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
    download_vedio_dir = config['DOWNLOAD_VEDIO_DIR']
    file_format = config['FILE_FORMAT']
    s3_bucket = config['S3_BUCKET']
    sheet_id = config['SHEET_ID']
    output_directory_16K = config['OUTPUT_DIRECTORY_16K']  # Directory for 16K WAV files

    # Read the spreadsheet
    df = read_spreadsheet(sheet_id=sheet_id)

    for index, row in df.iterrows():
        id = row[id_col]
        url_path = row[link_col]
        sr_no = row[sr_col]

        if sr_no >= from_id and sr_no <= to_id:
            video_filename = f"{download_vedio_dir}/{id}.mp4".strip()
            audio_filename = f"{download_audio_dir}/{id}.mp3".strip()

            print(id, url_path)
            if os.path.exists(audio_filename):
                print(f"Audio file {audio_filename} already exists. Skipping extraction.")
                continue
            if not os.path.exists(video_filename):
                download_url_file(url_path, video_filename)
            extract_audio(video_filename, audio_filename)


    # Convert MP3 to WAV files
    input_directory = download_audio_dir
    output_directory = f"{dept}_audio_wav"  # Replace with your desired output directory path
    batch_convert_mp3_to_wav(input_directory, output_directory)

    # Convert all WAV files to 16K sample rate
    convert_all_to_16K(output_directory, output_directory_16K)

    # Split the audio files
    split_audio_files(prefix, file_format, output_directory_16K, dept)

    # Collect the audio segments
    collect_segments(prefix, f'{dept}_after_split', segment_dir)

    # Upload the collected segments to the S3 bucket
    subprocess.run(f'aws s3 cp {segment_dir} {s3_bucket} --recursive', shell=True)

if __name__ == "__main__":
    # Parse arguments and load config
    config = parse_args_and_load_config()

    # Run the main pipeline logic
    main(config)
