import json
import os
import logging
from datetime import datetime
import argparse
from images_converter import convert_images
import uuid
import base64
import paho.mqtt.client as mqtt
from pathlib import Path

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

READY_SCREENSHOTS_DIR = Path("images_converter/data_dirs/ready_screenshots")

MQTT_BROKER = os.getenv('MQTT_BROKER', '5.35.107.131')  # Адрес вашего MQTT-брокера
MQTT_PORT = int(os.getenv('MQTT_PORT', 1883))          # Порт MQTT-брокера
MQTT_TOPIC_SNAPSHOT_BASE = os.getenv('MQTT_TOPIC_SNAPSHOT_BASE', 'snapshot_processing')  # Базовый топик для снимков
MQTT_TOPIC_UPLOAD_BASE = os.getenv('MQTT_TOPIC_UPLOAD_BASE', 'upload_video')              # Базовый топик для загрузки видео
MQTT_DLQ_TOPIC = os.getenv('MQTT_DLQ_TOPIC', 'dlq_event')                                 # Топик для DLQ (Dead Letter Queue)
MQTT_USERNAME = os.getenv('MQTT_USERNAME', 'civi-mq')  # Логин для MQTT
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD', 'Qwer1234!')  # Пароль для MQTT

client = mqtt.Client(client_id=f"python_mqtt_publisher_{uuid.uuid4()}")
client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Подключено к MQTT-брокеру")
    else:
        logger.error(f"Не удалось подключиться к MQTT-брокеру, код ошибки: {rc}")

client.on_connect = on_connect
def connect_mqtt():
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    except Exception as e:
        logger.error(f"Не удалось подключиться к MQTT-брокеру: {e}")
        raise e

    client.loop_start()

def check_event_error():
    directory = 'event_error'
    filename = 'failed_event.json'
    file_path = os.path.join(directory, filename)

    if os.path.isfile(file_path):
        logger.info(f'{filename} существует в {directory}')
        return True
    else:
        logger.info(f'{filename} не существует в {directory}')
        return False

def publish_message(topic, message_payload):
    try:
        result = client.publish(topic, payload=message_payload, qos=1)
        status = result[0]
        if status == mqtt.MQTT_ERR_SUCCESS:
            logger.info(f"Сообщение успешно опубликовано в топик {topic}")
        else:
            logger.error(f"Не удалось опубликовать сообщение в топик {topic}, код ошибки: {status}")
            raise Exception(f"Publish failed with status {status}")
    except Exception as e:
        logger.error(f"Ошибка при публикации сообщения: {e}")
        raise e

# def send_to_dlq(event_data):
#     message_payload = json.dumps(event_data)
#     try:
#         publish_message(MQTT_DLQ_TOPIC, message_payload)
#         logger.info(f"Сообщение отправлено в DLQ: {MQTT_DLQ_TOPIC}")
#     except Exception as e:
#         logger.error(f"Не удалось отправить сообщение в DLQ: {e}")



def read_all_json_files(directory_path):
    directory = Path(directory_path)

    # Check if directory exists
    if not directory.exists() or not directory.is_dir():
        logger.error(f"Directory {directory} does not exist or is not a directory.")
        return []

    json_data_list = []

    # Iterate over all JSON files in the directory
    for json_file in directory.glob("*.json"):
        try:
            with open(json_file, "r") as file:
                data = json.load(file)
                json_data_list.append(data)
                logger.info(f"Read data from {json_file}")
        except Exception as e:
            logger.error(f"Error reading file {json_file}: {e}")

    return json_data_list


# Function to process a new video event
def new_video(new_video_path, player_id):
    logger.info(f"Processing new video for player_id: {player_id}")
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%dT%H:%M:%S")
    directory_path = READY_SCREENSHOTS_DIR / new_video_path

    # Validate the directory exists
    if not directory_path.exists() or not directory_path.is_dir():
        logger.error(f"Directory {directory_path} does not exist or is not a directory.")
        return

    message_payload_list = read_all_json_files('./events_json')
    message_topic = f"{MQTT_TOPIC_SNAPSHOT_BASE}/{player_id}"

    # Publish each payload
    for message_payload in message_payload_list:
        try:
            publish_message(message_topic, message_payload)
            logger.info(f"Event successfully sent: player_id={player_id}, event_id={message_payload['id']}")
        except Exception as e:
            logger.error(f"Error sending event for player_id={player_id}, event_id={message_payload.get('id', 'N/A')}: {e}")



# Функция для отправки события удаления видео
def delete_video_event(new_video_path, cameraID):
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%dT%H:%M:%S")
    is_failed_exist = check_event_error()

    if is_failed_exist:
        with open('./event_error/failed_event.json', 'r') as failed_event_file:
            event_data = json.load(failed_event_file)
    else:
        event_data = {
            "id": str(uuid.uuid4()),  # Генерируем уникальный ID
            "playerid": 'c0:74:2b:fe:82:b4',
            "created_at": str(current_datetime),
            "typeid": 2,
            "starttime": str(formatted_datetime),
            "endtime": str(formatted_datetime),
            "duration": 300,
            "event": "update",
            "video_url": f"{new_video_path}",
            'cameraID': cameraID
        }

    message_payload = json.dumps(event_data)
    # Формируем уникальный топик для каждого player_id или cameraID
    # Здесь предполагается, что cameraID уникален и используется как идентификатор
    message_topic = f"{MQTT_TOPIC_UPLOAD_BASE}/{event_data['playerid']}"

    try:
        publish_message(message_topic, message_payload)
        logger.info(f'Событие удаления видео успешно отправлено: video_path={new_video_path}, cameraID={cameraID}')
    except Exception as e:
        logger.error(f"Ошибка при отправке события удаления видео: {e}")
        # Отправляем неудачное событие в DLQ
        # send_to_dlq(event_data)

if __name__ == '__main__':
    # Пример вызова функции new_video
    try:
        connect_mqtt()
        new_video(new_video_path='videos/c0:74:2b:fe:82:8e/9d4ecbe4-47b7-4614-ad7e-5b7057918700.mp4', player_id='c0:74:2b:fe:82:b4')
        # Пример вызова delete_video_event
        # delete_video_event(new_video_path='videos/c0:74:2b:fe:82:8e/9d4ecbe4-47b7-4614-ad7e-5b7057918700.mp4', cameraID='camera123')
    except Exception as e:
        logger.error(f"Основная программа завершилась с ошибкой: {e}")
    finally:
        client.loop_stop()
        client.disconnect()
