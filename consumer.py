import json
import os.path
from confluent_kafka import Consumer, KafkaError
from aws_connect import download_video
from Stage_1 import process_video, delete_video
from config import consumer_config
import logger
import shutil


base_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(base_dir)
grandparent_dir = os.path.dirname(parent_dir)
# files_dir = os.path.join(grandparent_dir, "civi-ai/screenshot_parser")
files_dir = os.path.join(grandparent_dir, "civi-ai/screenshot_parser")

temp_videos = os.path.join(files_dir, "converter/data_dirs/temp_videos")
screenshots = os.path.join(files_dir, "converter/data_dirs/ready_screenshots")


def consumer():
    consumer = Consumer(consumer_config)
    consumer.subscribe(['upload_video'])
    logger.system_log_logger.info('Starting consumer')
    no_message_count = 0
    max_no_message_count = 5
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                logger.system_log_logger.info('No messages received')
                continue
            no_message_count = 0
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('End of partition reached.')
                else:
                    print(f"Error occurred: {msg.error().str()}")
                continue
            message_key = msg.key()
            message_value = msg.value()
            if message_key is not None:
                message_key = message_key.decode('utf-8')
            if message_value is not None:
                message_value = message_value.decode('utf-8')
            else:
                logger.error_logs_logger.error("No message value, skipping this message.")
                continue

            logger.system_log_logger.info(f"Received message with key: {message_key} and value: {message_value}")
            event_data = json.loads(message_value)
            process_new_video_event(event_data)
            consumer.commit()

    except KeyboardInterrupt:
        print("Consumer interrupted. Shutting down...")
    except Exception as e:
        logger.error_logs_logger.error('Error occurred in upload_video event: %s' % e)
    finally:
        consumer.commit()
        consumer.close()


def process_new_video_event(event_data):
    if event_data["event"] == "upload":
        file_unique_id = os.path.splitext(os.path.basename(event_data['video_url']))[0]
        print(temp_videos, event_data["video_url"], file_unique_id)
        downloaded_file_path = download_video(temp_videos, event_data['video_url'], file_unique_id)

        # Verify if the video was downloaded correctly
        if os.path.exists(downloaded_file_path):
            process_video(input_path=downloaded_file_path,
                          output_path=screenshots,
                          player_id=event_data['playerid'])
            clean_temp_videos(downloaded_file_path)
        else:
            logger.system_log_logger.error(f'Failed to download video: {downloaded_file_path}')
    elif event_data["event"] == "delete":
        delete_video(event_data['video_id'])
    else:
        logger.system_log_logger.info(f'No event for this player ID: {os.getenv("PLAYER_ID")}')



def clean_temp_videos(path):
    if os.path.isfile(path):
        # If path is a file, delete it directly
        try:
            os.remove(path)
            print(f"File '{path}' has been deleted.")
        except Exception as e:
            print(f"Failed to delete file '{path}'. Reason: {e}")
    elif os.path.isdir(path):
        # If path is a directory, delete its contents
        for filename in os.listdir(path):
            file_path = os.path.join(path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.remove(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f"Failed to delete {file_path}. Reason: {e}")
        print(f"Directory '{path}' has been cleaned.")
    else:
        print(f"'{path}' does not exist.")


if __name__ == '__main__':
    process_new_video_event(event_data={'id': 1, 'playerid': 'c0:74:2b:fe:82:b4', 'created_at': '2024-10-27 21:20:56.629162', 'typeid': 2, 'starttime': '2024-10-27T21:20:56', 'endtime': '2024-10-27T21:20:56', 'duration': 300, 'event': 'upload', 'video_url': 'videos/c0:74:2b:fe:82:8e/9d23a920-39c0-455f-9368-801f2949a08e.mp4'})
