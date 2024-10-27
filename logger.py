import os
import logging
from datetime import datetime

base_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(base_dir)
grandparent_dir = os.path.dirname(parent_dir)
files_dir = os.path.join(grandparent_dir, "civi-ai/screenshot_parser")
print(files_dir)
user_count = None
system_log_file_path = os.path.join(files_dir, 'logs', 'logs.log')
actions_log_file_path = os.path.join(files_dir, 'logs', 'actions.log')
error_logs_log_file_path = os.path.join(files_dir, 'logs', 'error_logs.log')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')


class StaticFileFilter(logging.Filter):
    def filter(self, record):
        return not (
            'GET /static/' in record.getMessage() or
            'POST /process_form' in record.getMessage()
        )

def add_stream_handler(logger, level=logging.INFO):
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

# Setting up actions logger
actions_logger = logging.getLogger('actions_log')
actions_logger.setLevel(logging.WARNING)
actions_handler = logging.FileHandler(actions_log_file_path)
actions_handler.setFormatter(formatter)
actions_logger.addHandler(actions_handler)
add_stream_handler(actions_logger, logging.WARNING)  # Print actions logs to terminal


error_logs_logger = logging.getLogger('error_logs_log')
error_logs_logger.setLevel(logging.ERROR)
error_logs_handler = logging.FileHandler(error_logs_log_file_path)
error_logs_handler.setFormatter(formatter)
error_logs_logger.addHandler(error_logs_handler)
add_stream_handler(error_logs_logger, logging.ERROR)  # Print error logs to terminal
system_log_logger = logging.getLogger('system_logs_log')
system_log_logger.setLevel(logging.INFO)
system_log_handler = logging.FileHandler(system_log_file_path)
system_log_handler.setFormatter(formatter)
system_log_handler.addFilter(StaticFileFilter())
system_log_logger.addHandler(system_log_handler)
add_stream_handler(system_log_logger, logging.INFO)  # Print system logs to terminal
error_logs_logger.addHandler(system_log_handler)
