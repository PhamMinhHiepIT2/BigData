import pika

from rabbitmq.config import (
    HOST,
    PORT,
    EXCHANGE,
    ROUTING_KEY,
    RABBITMQ_USER,
    RABBITMQ_PASS
)


HEARTBEAT = 3600


def publish(msg):
    """
        Publish message to RabbitMQ queue
    """
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=HOST,
        port=PORT,
        credentials=credentials,
        virtual_host="/",
        heartbeat=HEARTBEAT)
    connection = pika.BlockingConnection(parameters=params)
    channel = connection.channel()
    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key=ROUTING_KEY,
        body=msg,
    )
    channel.close()
