# README

> **Note:** This readme template is based on one from the [Good Docs Project](https://thegooddocsproject.dev). You can find it and a guide to filling it out [here](https://gitlab.com/tgdp/templates/-/tree/main/readme). (_Erase this note after filling out the readme._)

<h1 align="center">
  <br>
  <a href="https://openpecha.org"><img src="https://avatars.githubusercontent.com/u/82142807?s=400&u=19e108a15566f3a1449bafb03b8dd706a72aebcd&v=4" alt="OpenPecha" width="150"></a>
  <br>
</h1>

## stt-split-audio

## Owner(s)

_Change to the owner(s) of the new repo. (This template's owners are:)_
- [@spsither](https://github.com/spsither)

## RFXs
Requests for work (RFWs) and requests for comments (RFCs) associated with this project:
* [RFW0051: pecha.tools for STT](https://github.com/OpenPecha/Requests/issues/175)
* [RFC0051: pecha.tools for STT](https://github.com/OpenPecha/Requests/issues/214)

## Table of contents
<p align="center">
  <a href="#project-description">Project description</a> •
  <a href="#who-this-project-is-for">Who this project is for</a> •
  <a href="#project-dependencies">Project dependencies</a> •
  <a href="#instructions-for-use">Instructions for use</a> •
  <a href="#contributing-guidelines">Contributing guidelines</a> •
  <a href="#additional-documentation">Additional documentation</a> •
  <a href="#how-to-get-help">How to get help</a> •
  <a href="#terms-of-use">Terms of use</a>
</p>
<hr>

## Project description

stt-split-audio helps you feed audio segments to stt.pecha.tools for annotation.


## Who this project is for
This project is intended for STT training data manager who wants to supply audio segments for annotation.


## Project dependencies
Before using stt-split-audio, ensure you have:
* Access to the catalog spread sheets.
* HuggingFace account and token.
* AWS credentials (aws_access_key_id, aws_secret_access_key) that has access to upload new segments to the monlam.ai.stt bucket
* Google Cloud account with access to the audio files in the catalog.


## Instructions for use
Get started with stt-split-audio by checking the catalog for a department and checking what id range to upload to the stt.pecha.tools.


### Install stt-split-audio
1. Get the Google Cloud Client secret to download files from Google Drive 

    Login to [Google Cloud Console](https://console.cloud.google.com/) and create a project.
   Click "API and services" > "Credentials" > "OAuth 2.0 Client IDs" > Download "Client secret" and rename it to credentials.json
   Upload the credentials.json file to **util** folder in **stt-split-audio**
    
    _(Optional: Include a code sample or screenshot that helps your users complete this step.)_

2. Install ffmpeg on EC2 if you are using EC2 Amazon Linux
    Use [this](https://www.maskaravivek.com/post/how-to-install-ffmpeg-on-ec2-running-amazon-linux/) link to install ffmpeg
    After following the steps from the above link also run 
    > ln -s /usr/local/bin/ffmpeg/ffprobe /usr/bin/ffprobe

3. Login to aws cli with 
    > aws configure

4. Database credentials 
    Create an .env file in util with the following environment variables 
    - HOST
    - DBNAME
    - DBUSER
    - PASSWORD

5. Ways to run the script. 
  Sample Commands for Individual Steps(for example youtube link download from new catalog):

    - first step cd to util directory: cd util 
    
    - Download YouTube videos and split into audio:

      bash code:
      ```
      python ../audio_download_and_split/yt_download.py --config ../json_config/nw_config.json
      ```
    - Run inference on audio segments:

      bash code:
      ```
      python ../inference_runner/run_inference.py --config ../json_config/nw_config.json
      ```
    - Generate CSV from inference results:

      bash code:
      ```
      python ../make_db_csv/make_csv.py --config ../json_config/nw_config.json
      ```
  How to Run the Entire Flow(for example youtube link download from new catalog):
    To run all the steps in one go, use your run_all.sh script, which executes each of the three key Python scripts (yt_download.py, run_inference.py, make_csv.py) sequentially. The script checks for errors after each step and stops if an error is encountered.

    Ensure your environment is set up:

      Google Cloud credentials are in place.
      Required Python libraries are installed.
      AWS CLI is configured and authenticated.
      go to util directory: cd util/
      Run the shell script: ../run_all.sh
## implementation flow
![image](https://github.com/user-attachments/assets/147443db-60b3-4f7c-af54-b47e0ecea799)

## Transfer Text Function
### Overview
The transfer_text function aligns and transfers annotations from predicted text (in a TSV file) to the original text (in a text file). The output is a DataFrame containing transferred annotations, ensuring a one-to-one correspondence between the predicted and original text.


## Contributing guidelines
If you'd like to help out, check out our [contributing guidelines](/CONTRIBUTING.md).


## How to get help
* File an issue.
* Email us at openpecha[at]gmail.com.
* Join our [discord](https://discord.com/invite/7GFpPFSTeA).


## Terms of use
stt-split-audio is licensed under the [MIT License](/LICENSE.md).
