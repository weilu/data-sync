import boto3
from tarfile import TarFile
import os
import dropbox
import logging
import shutil
from pathlib import Path
from tqdm import tqdm
from dropbox.exceptions import ApiError, InternalServerError
import traceback
from time import sleep


logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s',
                    level=logging.INFO)

# https://stackoverflow.com/a/37399658/429288
def upload(
    dbx,
    file_path,
    target_path,
    chunk_size=4 * 1024 * 1024,
):
    with open(file_path, "rb") as f:
        file_size = os.path.getsize(file_path)
        chunk_size = 4 * 1024 * 1024
        if file_size <= chunk_size:
            print(dbx.files_upload(f.read(), target_path, mute=True))
        else:
            with tqdm(total=file_size, desc="Uploaded") as pbar:
                upload_session_start_result = dbx.files_upload_session_start(
                    f.read(chunk_size)
                )
                pbar.update(chunk_size)
                cursor = dropbox.files.UploadSessionCursor(
                    session_id=upload_session_start_result.session_id,
                    offset=f.tell(),
                )
                commit = dropbox.files.CommitInfo(path=target_path)
                while f.tell() < file_size:
                    if (file_size - f.tell()) <= chunk_size:
                        print(
                            dbx.files_upload_session_finish(
                                f.read(chunk_size), cursor, commit
                            )
                        )
                    else:
                        dbx.files_upload_session_append(
                            f.read(chunk_size),
                            cursor.session_id,
                            cursor.offset,
                        )
                        cursor.offset = f.tell()
                    pbar.update(chunk_size)


def exist_on_dropbox(dbx, dropbox_path):
    try:
        dbx.files_get_metadata(dropbox_path)
        return True
    except:
        return False


def upload_with_retry(dbx, local_path, dropbox_path, retried=0):
    if retried:
        sleep_seconds = 10**retried
        logging.warning(f'uploading of {dropbox_path} failed. At retry #{retried}. Will sleep for {sleep_seconds} before retry')
        sleep(sleep_seconds)

    try:
        upload(dbx, local_path, dropbox_path)
    except ApiError as e:
        traceback.print_exc()
        logging.warning(f'uploading of {dropbox_path} failed due to ApiError. Skipping...')
    except InternalServerError as e:
        traceback.print_exc()
        if retried >= 4:
            logging.warning(f'uploading of {dropbox_path} failed after {retried} retries. Existing...')
            raise e
        else:
            return upload_with_retry(dbx, local_path, dropbox_path, retried+1)


S3_BUCKET_NAME= os.environ.get('S3_BUCKET_NAME')
DROPBOX_TOKEN = os.environ.get('DROPBOX_TOKEN')
dbx = dropbox.Dropbox(DROPBOX_TOKEN, timeout=900)

s3 = boto3.client('s3')
response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME)
all_files = [(c['Key'], c['Size']) for c in response['Contents']]

MAX_FILE_SIZE_GB = os.environ.get('MAX_FILE_SIZE_GB')

for filename, size in all_files:
    if not filename.endswith('.tar'):
        continue

    done_file_path = os.path.join('done', filename)
    if os.path.exists(done_file_path):
        logging.info(f'Skipping done {filename}')
        continue

    # only process files smaller than MAX_FILE_SIZE_GB
    if MAX_FILE_SIZE_GB:
        if size > float(MAX_FILE_SIZE_GB) * 1000 * 1000 * 1000:
            logging.info(f'Skipping {filename} due to large size {size}')
            continue

    if not os.path.exists(filename):
        logging.info(f'Downloading {filename} from s3')
        s3.download_file(S3_BUCKET_NAME, filename, filename)

    if not os.path.exists('tosync'):
        tar = TarFile(filename)
        logging.info(f'Untaring {filename}')
        tar.extractall('tosync')

    dropbox_top_dir = None
    for root, dirs, files in os.walk('tosync'):
        if not dropbox_top_dir: # only set it once using the first value discovered
            # special handling for textlab_data_db_exports.tar
            if root == 'tosync/data' and len(dirs) == 1 and dirs[0] == 'db_exports':
                rel_depth = '../..'
            else:
                rel_depth = '../../..'
            relpath_to_root = os.path.relpath(dirs[0], root)
            if rel_depth in relpath_to_root:
                dropbox_top_dir = dirs[0]
                relpath_start = root.replace(dropbox_top_dir, '')
                print(f'dropbox_top_dir: {dropbox_top_dir}, relpath_start: {relpath_start}')

        for to_upload_filename in files:
            filename_lower = to_upload_filename.lower()
            if filename_lower.endswith('.dropbox') or filename_lower.endswith('.ds_store'):
                print(f'skipping disallowed file: {to_upload_filename}')
                continue
            local_path = os.path.join(root, to_upload_filename)
            if not os.path.isfile(local_path):
                print(f'skipping non-file: {local_path}')
                continue

            dropbox_path = '/data_sync-' + os.path.relpath(local_path, relpath_start)

            if exist_on_dropbox(dbx, dropbox_path):
                logging.info(f'Skipping {local_path} as it exists on dropbox')
            else:
                logging.info(f'Uploading from local: {local_path} to dropbox: {dropbox_path}')
                upload_with_retry(dbx, local_path, dropbox_path)


    shutil.rmtree('tosync')
    os.remove(filename)
    Path(done_file_path).touch()
    logging.info(f'Done unpacking {filename} to /data_sync-{dropbox_top_dir}')

