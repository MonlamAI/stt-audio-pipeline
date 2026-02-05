import os
import sys
import subprocess

sys.path.append('../util')

from google_utils import read_spreadsheet
from files_utils import collect_segments
from audio_utils import split_audio_files, convert_all_to_16K, batch_convert_mp3_to_wav
from common_utils import parse_args_and_load_config
from download_utils import download_audio_url_header


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
    output_directory_16K = config['OUTPUT_DIRECTORY_16K']  # Directory for 16K WAV files

    # Headers for the audio download request
    # Define the headers (as given)
    headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9,en-IN;q=0.8",
    "cache-control": "max-age=0",
    "cookie": "AMCVS_518ABC7455E462B97F000101%40AdobeOrg=1; s_cc=true; utag_main=v_id:019169eade56002296e6ea4a443c0507d001b075008f7$_sn:11$_se:5$_ss:0$_st:1727788387180$vapi_domain:rfa.org$ses_id:1727793787%3Bexp-session$_pn:5%3Bexp-session; AMCV_518ABC7455E462B97F000101%40AdobeOrg=1176715910%7CMCIDTS%7C19997%7CMCMID%7C92058839809215745258174654077801968713%7CMCAID%7CNONE%7CMCOPTOUT-1727793787s%7CNONE%7CvVersion%7C5.4.0; s_sq=%5B%5BB%5D%5D",
    "priority": "u=0, i",
    "sec-ch-ua": "\"Microsoft Edge\";v=\"129\", \"Not=A?Brand\";v=\"8\", \"Chromium\";v=\"129\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0"
}


    # Ensure the download directory exists
    if not os.path.exists(download_audio_dir):
        os.makedirs(download_audio_dir)
        print(f"Created directory: {download_audio_dir}")
    
    # Read the spreadsheet
    df = read_spreadsheet(sheet_id=sheet_id)

    for index, row in df.iterrows():
        id = row[id_col]
        url_path = row[link_col]
        sr_no = row[sr_col]

        if sr_no >= from_id and sr_no <= to_id:
            audio_filename = f"{download_audio_dir}/{id}.mp3".strip()

            print(id, url_path)
            if os.path.exists(audio_filename):
                print(f"Audio file {audio_filename} already exists. Skipping extraction.")
                continue
            if not os.path.exists(audio_filename):
                download_audio_url_header(url_path, headers, audio_filename)

    # Convert MP3 to WAV files
    input_directory = download_audio_dir
    output_directory = f"../{dept}_audio_wav"  # Replace with your desired output directory path
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
