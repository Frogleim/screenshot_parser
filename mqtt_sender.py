import json
import os
import random
import logging
import time
from uuid import uuid4
from datetime import datetime
import traceback
import zlib  # Для сжатия данных
import paho.mqtt.client as mqtt

from logger import print_and_log, print_and_log_exception

EXCEPTION = False

def insert_data(event_objects):
    global EXCEPTION

    try:
        EXCEPTION = False

        # Настройки MQTT-брокера
        mqtt_broker = '5.35.107.131'
        mqtt_port = 1883  # Стандартный порт MQTT
        mqtt_topic = 'test'
        mqtt_username = 'civi-mq'  # Замените на ваш логин
        mqtt_password = 'Qwer1234!'  # Замените на ваш пароль

        # Инициализация MQTT-клиента
        client = mqtt.Client()
        client.username_pw_set(mqtt_username, mqtt_password)
        client.connect(mqtt_broker, mqtt_port, 60)

        print_and_log(logging.INFO, 'Соединение с MQTT-брокером установлено!')

        types_ = {'car': 1, 'truck': 2, 'bus': 3, 'person': 4, 'service': 5}

        # Список для накопления событий
        events_list = []

        for event_object in event_objects:
            type_id = types_.get(event_object.class_name, None)
            if type_id is None:
                raise ValueError(f"Неизвестный тип события: {event_object.class_name}")
            player_id = event_object.player_id
            event_id = str(uuid4())

            # Оптимизированные данные для отправки
            event_data = {
                'Id': event_id,
                'TId': type_id,
                'ST': int(event_object.time_start),
                'Dur': event_object.get_duration(),
                'PID': player_id,
            }

            if event_object.class_name in ['car', 'bus', 'truck']:
                car_values = {
                    'CId': event_object.class_id,
                    'MName': 'CarModel',
                    'CName': event_object.class_name,
                }
                event_data.update(car_values)
            elif event_object.class_name == 'service':
                service_values = {
                    'Det': event_object.detector,
                }
                event_data.update(service_values)
            elif event_object.class_name == 'person':
                person_values = {
                    'Sex': 'M' if random.randint(0, 1) == 0 else 'F',
                    'Age': random.randint(10, 70),
                }
                event_data.update(person_values)

            if hasattr(event_object, 'location_id') and hasattr(event_object, 'area'):
                event_area_values = {
                    'LId': event_object.location_id,
                    'Area': event_object.area,
                }
                event_data.update(event_area_values)

            events_list.append(event_data)

        print_and_log(logging.INFO, 'Создание JSON с данными!')

        # Разбиваем список событий на батчи по 100 элементов
        batch_size = 100
        for i in range(0, len(events_list), batch_size):
            batch_events = events_list[i:i + batch_size]
            message_payload = json.dumps(batch_events, separators=(',', ':'))
            compressed_payload = zlib.compress(message_payload.encode('utf-8'))

            # Публикуем сообщение на MQTT-брокер
            client.publish(mqtt_topic, compressed_payload, qos=0)

            print_and_log(logging.INFO, f'Отправлен батч с {len(batch_events)} событиями на MQTT-брокер.')

        # Отключаемся от брокера
        client.disconnect()
        print_and_log(logging.INFO, f'Все данные отправлены на MQTT-брокер! Error: {EXCEPTION}')
    except Exception as e:
        print_and_log_exception(f"Ошибка при отправке данных на MQTT-брокер: \n{traceback.format_exc()}")
        EXCEPTION = True