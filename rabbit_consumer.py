import pika
import json
import os
import logger
from aws_connect import download_video
from Stage_1 import process_video, delete_video
from config import rabbitmq_config
import shutil

base_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(base_dir)
grandparent_dir = os.path.dirname(parent_dir)
files_dir = os.path.join(grandparent_dir, "app")
temp_videos = os.path.join(files_dir, "data_dirs/temp_videos/")
screenshots = os.path.join(files_dir, "data_dirs/ready_screenshots/")

# Process messages received
def process_new_video_event(event_data):
    # Process based on event type
    if event_data["playerid"] == os.getenv("PLAYER_ID"):
        if event_data["event"] == "upload":
            download_video(temp_videos, event_data['video_url'])
            file_unique_id = os.path.splitext(os.path.basename(event_data['video_url']))[0]
            process_video(input_path=f'{temp_videos}/{file_unique_id}.mp4', s3_file_path=event_data['video_url'],
                          output_path=f'{screenshots}/ready_screenshots', cameraID=event_data['playerid'])
            clean_temp_videos(dir_path=f'{temp_videos}/{file_unique_id}.mp4')
        elif event_data["event"] == "delete":
            delete_video(event_data['video_id'])
    else:
        logger.system_log_logger.info(f'There is no event for this player id {os.getenv("PLAYER_ID")}')


# Setup RabbitMQ Consumer
def rabbitmq_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=rabbitmq_config['host'],
        port=rabbitmq_config['port'],
        virtual_host=rabbitmq_config['vhost'],
        credentials=pika.PlainCredentials(rabbitmq_config['user'], rabbitmq_config['password'])
    ))
    channel = connection.channel()

    # Declare the queue (it should already exist on the RabbitMQ server)
    channel.queue_declare(queue='upload_video', durable=True)

    # Callback function to process each message
    def callback(ch, method, properties, body):
        try:
            logger.system_log_logger.info(f"Received message: {body}")
            message = json.loads(body)
            process_new_video_event(message)
            # Acknowledge message processing
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.error_logs_logger.error(f"Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    # Start consuming messages
    channel.basic_consume(queue='upload_video', on_message_callback=callback)
    logger.system_log_logger.info("Starting RabbitMQ consumer...")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.system_log_logger.info("Consumer interrupted. Shutting down...")
    finally:
        channel.stop_consuming()
        connection.close()



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