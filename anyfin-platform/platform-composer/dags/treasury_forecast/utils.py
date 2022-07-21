from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from os import listdir

# Separate file for these functions because PythonVirtualenvOperator doesn't see any variables and functions
# defined outside the python_callable function.


def upload_csv_to_gdrive(creds, gdrive_folder_id, local_path_to_files='', file_names=None):
    """
    Upload list of provided file_names to folder_id. Expected that all files from file_names are stored in
    local_path_to_files. Loads only csv files

    Args:
        creds: ServiceAccountCredentials
            Object with credentials and all necessary scopes
        gdrive_folder_id: str
            id of gdrive folder where data should be stored
        local_path_to_files: str (default '' for cases if file is stored in the same location where code executes)
            full path to files from file_names list
        file_names: list[str] (default None for cases if all files from local_path_to_files should be uploaded)
            list of files to load to gdrive. It should contain only file names without the path.

    Raises:
        ValueError if both local_path_to_files and file_names are not provided

    Returns:
        list: uploaded files ids
    """

    if local_path_to_files == '' and file_names is None:
        raise ValueError("One of local_path_to_files and file_names should be not empty. You provided "
                         "local_path_to_files='', file_names=None")

    if file_names is None:
        file_names = listdir(local_path_to_files)

    files_uploaded = []
    try:
        service = build('drive', 'v3', credentials=creds, cache_discovery=False)

        all_files = []
        page_token = ""
        while page_token is not None:
            response = service.files().list(q="'" + gdrive_folder_id + "' in parents", pageSize=1000,
                                            pageToken=page_token,
                                            fields="nextPageToken, files(id, name)").execute()
            all_files.extend(response.get('files', []))
            page_token = response.get('nextPageToken')

        for file_name in file_names:
            file_metadata = {'name': file_name, 'parents': [gdrive_folder_id]}
            media = MediaFileUpload(f'{local_path_to_files}{file_name}',
                                    mimetype='text/csv')

            file_id = None
            if file_name in [f['name'] for f in all_files]:
                file_id = [f['id'] for f in all_files if f['name'] == file_name][0]
            if file_id:
                file = service.files().update(fileId=file_id, body=None, media_body=media).execute()
            else:
                file = service.files().create(body=file_metadata, media_body=media,
                                              fields='id').execute()
            files_uploaded.append(file.get('id'))
    except HttpError as error:
        print(f'Error occurred: {error}')

    return files_uploaded


def download_settings_file(creds, sheet_url, data_storage_path):
    '''
    Export excel file from gdrive to provided path
    Args:
        creds: ServiceAccountCredentials
            Object with credentials and all necessary scopes
        sheet_url: str
            Full url of the Google sheet which needs to be downloaded. Part after id can be skipped
        data_storage_path: str
            Path to where store the Google sheet

    Returns:
        None
    '''
    import gspread as gs
    from gspread.utils import ExportFormat

    gc = gs.authorize(creds)
    sh = gc.open_by_url(sheet_url)

    with open(f'{data_storage_path}Model settings.xlsx', 'wb') as f:
        f.write(sh.export(ExportFormat.EXCEL))
