import os
import sys
import subprocess

sys.path.append('../util')


from common_utils import get_time_span
from common_utils import clean_transcription
from transformers import pipeline
from tqdm.auto import tqdm
import pandas as pd
from pathlib import Path
from common_utils import parse_args_and_load_config


def main(config):
    # Read configuration from the loaded JSON
    dept = config['DEPARTMENT']
    segment_dir = config['SEGMENT_DIR']
    target_path = Path(segment_dir)
    rows = []

    generator = pipeline(model="ganga4364/mms_300_v4.96000")

    for file in tqdm(target_path.glob('*.wav'), total=len(list(target_path.glob('*.wav')))):
        inf = generator(str(file))["text"]
        rows.append([file.stem, f"https://d38pmlk0v88drf.cloudfront.net/wav16k/{file.name}", inf, get_time_span(str(file.name))])


    df = pd.DataFrame(rows, columns =['file_name', 'url', 'inference_transcript', 'audio_duration'])
    df = df.sort_values('file_name').reset_index(drop=True)
    df[['inference_transcript','url']].iloc[0:10].to_dict()
    df['inference_transcript'] = df['inference_transcript'].map(clean_transcription)
    df.to_csv(f"../data/{dept}.csv", index=False)

if __name__ == "__main__":
    # Parse arguments and load config
    config = parse_args_and_load_config()

    # Run the main pipeline logic
    main(config)
