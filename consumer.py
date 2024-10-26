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
files_dir = os.path.join(grandparent_dir, "app")
temp_videos = os.path.join(files_dir, "data_dirs/temp_videos/")
screenshots = os.path.join(files_dir, "data_dirs/ready_screenshots/")


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
    if event_data["playerid"] == os.getenv("PLAYER_ID"):
        if event_data["event"] == "upload":
            download_video(temp_videos, event_data['video_url'])
            file_unique_id = os.path.splitext(os.path.basename(event_data['video_url']))[0]

            process_video(input_path=f'{temp_videos}/{file_unique_id}.mp4',
                          output_path=f'{screenshots}/ready_screenshots', player_id=event_data['playerid'])
            clean_temp_videos(dir_path=f'{temp_videos}/{file_unique_id}.mp4')
        elif event_data["event"] == "delete":
            delete_video(event_data['video_id'])
    else:
        logger.system_log_logger.info(f'There is no event for this player id {os.getenv("PLAYER_ID")}')


def clean_temp_videos(dir_path):
    if os.path.exists(dir_path):
        for filename in os.listdir(dir_path):
            file_path = os.path.join(dir_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.remove(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f"Failed to delete {file_path}. Reason: {e}")
        print(f"Directory '{dir_path}' has been cleaned.")
    else:
        print(f"Directory '{dir_path}' does not exist.")


if __name__ == '__main__':
    consume_new_video_event()
