import json
import os
import logger
from confluent_kafka import Producer
from datetime import datetime
from config import params
import argparse
from converter import convert_images

p = Producer(params)


def check_event_error():
    directory = 'event_error'
    filename = 'failed_event.json'
    file_path = os.path.join(directory, filename)

    if os.path.isfile(file_path):
        print(f'{filename} exist in {directory}')
        return True
    else:
        print(f'{filename} does not exist in {directory}')
        return False

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def new_video(new_vido_path, player_id):

    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%dT%H:%M:%S")
    is_failed_exist = check_event_error()
    directory_path = "./data_dirs/ready_screenshots"
    images_base64_dict = convert_images.images_to_base64(directory_path)
    for filename, base64_string in images_base64_dict.items():
        logger.system_log_logger.info(f"{filename}: {base64_string[:10]}...")  # Printing only the first 30 characters for brevity

        if is_failed_exist:
            with open('./event_error/failed_event.json', 'w') as failed_event:
                event_data = json.load(failed_event)
        else:
            event_data = {
                "id": 1,
                "playerid": player_id,
                "created_at": str(current_datetime),
                "typeid": 2,
                "starttime": str(formatted_datetime),
                "endtime": str(formatted_datetime),
                "duration": 300,
                "event": "new_video",
                "video_url": f"{new_vido_path}",
                "screenshot": base64_string
            }

        message_payload = json.dumps(event_data)
        message_key = str(event_data["id"])
        try:
            p.produce('snapshot_processing', key=message_key, value=message_payload, callback=delivery_report)
            p.flush(10)
            print(p.list_topics())
        except Exception as e:
            print(f"Error while sending event!\n{e}")
            with open('failed_event', 'w') as f:
                json.dump(event_data, f)

def delete_video_event(new_vido_path, cameraID):
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%dT%H:%M:%S")
    is_failed_exist = check_event_error()

    if is_failed_exist:
        with open('./event_error/failed_event.json', 'w') as failed_event:
            event_data = json.load(failed_event)
    else:
        event_data = {
            "id": 1,
            "playerid": 'c0:74:2b:fe:82:b4',
            "created_at": str(current_datetime),
            "typeid": 2,
            "starttime": str(formatted_datetime),
            "endtime": str(formatted_datetime),
            "duration": 300,
            "event": "update",
            "video_url": f"{new_vido_path}",
            'cameraID': cameraID
        }
    message_payload = json.dumps(event_data)
    message_key = str(event_data["id"])
    try:
        p.produce('upload_video', key=message_key, value=message_payload)
        p.flush(10)
        print(p.list_topics())

    except Exception as e:
        print(f"Error while sending event!\n{e}")
        with open('failed_event', 'w') as f:
            json.dump(event_data, f)



if __name__ == '__main__':


    new_video(new_vido_path='videos/c0:74:2b:fe:82:8e/9d4ecbe4-47b7-4614-ad7e-5b7057918700.mp4')
