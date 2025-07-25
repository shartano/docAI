import os.path
import io

from auth import getCredentials
import pdfplumber

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

def build_drive_service(creds):
    return build("drive", "v3", credentials=creds)

def list_file_in_folder(service, folder_id, drive_id=None):
    query = f"'{folder_id}' in parents"
    page_token = None

    try:
        while True:
            response = service.files().list(
                q=query,
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token,
                corpora="drive",
                driveId=drive_id,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True
            ).execute()

            for file in response.get("files", []):
                file_id = file["id"]
                file_name = file["name"]
                mime_type = file["mimeType"]

                if mime_type == "application/vnd.google-apps.folder":
                    print(f"\nEntering Folder: {file_name}")
                    list_file_in_folder(service, file_id, drive_id)
                else:
                    print(f"Processing file: {file_name}")
                    file_obj = download_file_content(service, file_id)
                    if file_obj:
                        text = extract_text(file_obj, mime_type)
                        if text:
                            print(f"Extracted {len(text)} chars from: {file_name}")
                            # TODO: Embed & insert into your DB

            page_token = response.get("nextPageToken", None)
            if page_token is None:
                break

    except HttpError as error:
        print(f"API Error: {error}")



def download_file_content(service, file_id):
    try:
        request = service.files().get_media(fileId = file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            _, done = downloader.next_chunk()

        fh.seek(0)
        return fh
    except HttpError as error:
        print(f"Error downloading `{error}`")






def extract_text(file_obj, mime_type):
    try:
        match mime_type:
            case 'text/plain':
                return file_obj.read().decode("utf-8")
            case "application/pdf":
                with pdfplumber.open(file_obj) as pdf:
                    return "\n".join(page.extract_text() or '' for page in pdf.pages)
                
                #add more file type conversion here
            case _:
                print(f"Unsupported mime type: {mime_type}")
                return ""
    except Exception as e:
        print(f"Errot reading file: {e}")
        return ""
    
