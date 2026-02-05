import os
from pathlib import Path
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import pandas as pd

def read_spreadsheet(sheet_id):
    """
    Reads a Google Spreadsheet as a Pandas DataFrame.
    
    Args:
        sheet_id (str): The ID of the Google Spreadsheet.
    
    Returns:
        DataFrame: A Pandas DataFrame containing the spreadsheet data.
    """
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv"
    df = pd.read_csv(url)
    return df


def create_drive_service():
    """
    Creates a Google Drive service object for interacting with the Google Drive API.
    
    Returns:
        Resource: A Google Drive API service resource.
    """
    creds = None
    if os.path.exists("../util/token.json"):
        creds = Credentials.from_authorized_user_file("../util/token.json")
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "../util/credentials.json", ["https://www.googleapis.com/auth/drive"]
            )
            creds = flow.run_local_server(port=0)
        with open("../util/token.json", "w") as token:
            token.write(creds.to_json())
    return build("drive", "v3", credentials=creds)


# Create the drive service
drive_service = create_drive_service()

def download_audio_gdrive(gd_url, file_name, output_folder):
    """
    Downloads an audio file from Google Drive to a local directory.
    
    Args:
        gd_url (str): The Google Drive URL or file ID.
        file_name (str): The desired name of the downloaded file.
    """
    Path(output_folder).mkdir(parents=True, exist_ok=True)

    if Path(output_folder, file_name).exists():
        print(f"File {file_name} already exists.")
        return

    file_id = gd_url.split("/")[-2] if "drive.google.com" in gd_url else gd_url
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    
    done = False
    while not done:
        status, done = downloader.next_chunk()
        print(f"Download {int(status.progress() * 100)}%.")

    with open(f"{output_folder}/{file_name}", "wb") as f:
        f.write(fh.getbuffer())
        print(f"File {file_name} downloaded successfully.")

