import cv2
import os
import logger
import producer
import uuid
import json
from datetime import datetime
import time
from pathlib import Path
import logger
from images_converter import convert_images

READY_SCREENSHOTS_DIR = Path("images_converter/data_dirs/ready_screenshots")


def save_event(file_unique_id, player_id, start_time, end_time):
    directory_path = READY_SCREENSHOTS_DIR / file_unique_id

    images_base64_dict = convert_images.images_to_base64(directory_path)

    for filename, base64_string in images_base64_dict.items():
        logger.system_log_logger.info(f"{file_unique_id}: {filename}: {base64_string[:10]}...")  # Печатаем первые 10 символов

        event_id = str(uuid.uuid4())
        current_datetime = datetime.now()
        event_data = {
            "id": event_id,
            "playerid": player_id,
            "created_at": str(current_datetime),
            "typeid": 2,
            "starttime": str(start_time),
            "endtime": str(end_time),
            "duration": 300,
            "event": "new_video",
            "video_id": file_unique_id,
            "screenshot": base64_string
        }
        output_file = f"{file_unique_id}.json"
        with open(f'events_json/{output_file}', "w") as json_file:
            json.dump(event_data, json_file, indent=4)



def process_video(input_path,  output_path, player_id,   start_time, end_time, level=1):
    file_unique_id = os.path.splitext(os.path.basename(input_path))[0]
    logger.system_log_logger.info(file_unique_id)
    try:
        os.mkdir(f'{output_path}/{file_unique_id}')
    except FileExistsError:
        logger.system_log_logger.info(f"Video already exist {output_path}")
        producer.new_video(file_unique_id, player_id)
        return
    cap = cv2.VideoCapture(input_path)
    if not cap.isOpened():
        logger.system_log_logger.info(f"Не удалось открыть видеофайл {input_path}")
        return
    fps = cap.get(cv2.CAP_PROP_FPS)
    frame_count = 0
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    while True:
        ret, frame = cap.read()
        if not ret:
            break
        if frame_count % int(fps) == 0:
            frame_time = int(frame_count // fps)
            filename = os.path.join(output_path, f"{file_unique_id}/frame_{frame_time}.jpg")
            cv2.imwrite(filename, frame)

        frame_count += 1
    cap.release()
    logger.system_log_logger.info(f"Видео обработано. Скрины сохранены в {output_path}")
    save_event(file_unique_id, player_id, start_time, end_time)
    producer.new_video(file_unique_id, player_id)
    return

def delete_video(video_id):
    if not video_id:
        logger.error_logs_logger.error('Video id is required')
    base_dir = '/images_converter/data_dirs/ready_screenshots'
    video_id_dir = os.path.join(base_dir, video_id)
    os.rmdir(video_id_dir)
    logger.system_log_logger.info(f'Video screenshots with id: {video_id} deleted')




if __name__ == '__main__':
    process_video(input_path='images_converter/data_dirs/temp_videos/01J8SX71BZZWHB4GQ1Y6Z4S6FQ.mp4', output_path='../data_dirs/ready_screenshots')
