import os.path
from config import session
import time
import os
import logger

videos_count = 17
tracked_files = set()

s3 = session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.kz'  # Custom S3-compatible endpoint
)

bucket_name = 'civi-led'
all_files = []


def download_video(path, file_name, file_unique_id):
    # Generate the target download path
    downloaded_file_name = f'{path}/{file_unique_id}.mp4'
    if not os.path.exists(downloaded_file_name):
        try:
            # Download video from S3
            s3.download_file(bucket_name, file_name, downloaded_file_name)
            print(f'Video downloaded successfully to {downloaded_file_name}')
        except FileNotFoundError as e:
            logger.system_log_logger.error(f'Error in download_video: {e}')
            raise
    else:
        print(f'File already exists at {downloaded_file_name}')
    return downloaded_file_name



def get_s3_file_list():
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix='/Test_video')
    return {item['Key'] for item in response.get('Contents', [])}


def create_dir(dir_name):
    s3.put_object(Bucket=bucket_name, Key=dir_name)

def check_for_changes():
    global tracked_files

    current_files = get_s3_file_list()
    print(current_files)

    new_files = current_files - tracked_files
    if new_files:
        print(f'New files detected: {tracked_files}')

    deleted_files = tracked_files - current_files
    if deleted_files:
        print(f'Files Deleted: {deleted_files}')

    tracked_files = current_files
    print('No changes Detected')


def monitor_storage(interval=60):
    while True:
        check_for_changes()
        time.sleep(interval)





if __name__ == '__main__':
    # tracked_files = get_s3_file_list()
    # print(f'Initial files: {tracked_files}')
    #
    # monitor_storage(interval=60)
    # upload_video('./videos/video.mov', bucket_name=bucket_name, name_for_s3='videos/test.mp4')
    # data = get_s3_file_list()
    # print(data)
    # delete_files(bucket_name=bucket_name, name_for_s3='test', prefix='videos')
    download_video('images_converter/data_dirs/temp_videos/', 'videos/c0:74:2b:fe:83:b0/9d23a920-39c0-455f-9368-801f2949a08e.mp4', '9d23a920-39c0-455f-9368-801f2949a08e')
    # create_dir(dir_name='Test_video')