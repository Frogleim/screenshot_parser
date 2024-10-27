import cv2
import os
import logger
import producer


def process_video(input_path,  output_path, player_id,  level=1):
    file_unique_id = os.path.splitext(os.path.basename(input_path))[0]
    logger.system_log_logger.info(file_unique_id)
    os.mkdir(f'{output_path}/{file_unique_id}')

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
    producer.new_video(file_unique_id, player_id)

def delete_video(video_id):
    if not video_id:
        logger.error_logs_logger.error('Video id is required')
    base_dir = '../data_dirs/ready_screenshots'
    video_id_dir = os.path.join(base_dir, video_id)
    os.rmdir(video_id_dir)
    logger.system_log_logger.info(f'Video screenshots with id: {video_id} deleted')




if __name__ == '__main__':
    process_video(input_path='converter/data_dirs/temp_videos/01J8SX71BZZWHB4GQ1Y6Z4S6FQ.mp4', output_path='../data_dirs/ready_screenshots')
