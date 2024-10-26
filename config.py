import boto3
import os


session = boto3.Session(
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    region_name='kz1'
)
s3 = session.client(
    service_name='s3',
    endpoint_url=os.environ['ENDPOINT_URL']
)



def error_callback(err):
    print('Something went wrong: {}'.format(err))


params = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL'),
    'ssl.ca.location': os.getenv('KAFKA_SSL_CA_LOCATION'),
    'sasl.mechanism': os.getenv('KAFKA_SASL_MECHANISM'),
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
    'error_cb': error_callback,  # Callback for errors
}


consumer_config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL'),
    'ssl.ca.location': os.getenv('KAFKA_SSL_CA_LOCATION'),
    'sasl.mechanism': os.getenv('KAFKA_SASL_MECHANISM'),
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
    'group.id': 'video-event-consumer-group',  # Unique group for this consumer
    'auto.offset.reset': 'earliest',  # Start reading at the earliest message
    'error_cb': error_callback,  # Callback for errors
}


rabbitmq_config = {
    'host': os.getenv('RABBITMQ_HOST'),
    'port': os.getenv('RABBITMQ_PORT'),
    'vhost': os.getenv('RABBITMQ_VHOST'),
    'user': os.getenv('RABBITMQ_USER'),
    'password': os.getenv('RABBITMQ_PASSWORD'),
}