#!/bin/bash
# Run yt_download.py
echo "Running docx_download.py..."
python ../audio_download_and_split/yt_download.py --config ../json_config/cs_config.json
if [ $? -ne 0 ]; then
  echo "Error running yt_download.py. Exiting..."
  exit 1
fi
# Run run_inference.py
echo "Running run_inference.py..."
python ../inference_runner/run_inference.py --config ../json_config/cs_config.json
if [ $? -ne 0 ]; then
  echo "Error running run_inference.py. Exiting..."
  exit 1
fi
# Run make_csv.py
echo "Running make_csv.py..."
python ../make_db_csv/make_csv.py --config ../json_config/cs_config.json
if [ $? -ne 0 ]; then
  echo "Error running make_csv.py. Exiting..."
  exit 1
fi
echo "All scripts ran successfully!"
"""
# Run transfer_text.py
echo "Running transfer_text.py..."
python ../make_db_csv/transfer_text.py --config ../json_config/ab_config.json
if [ $? -ne 0 ]; then
  echo "Error running make_csv.py. Exiting..."
  exit 1
fi
echo "All scripts ran successfully!"
"""