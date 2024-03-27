from typing import Union
import threading
import importlib
import functools

from ..core.message_queue import message_queue, event_queue
from ..core.env_reader import env_reader


class Connector:
    def __init__(self, connection_type: str = "kafka") -> None:
        connection_handler_module = importlib.import_module(
            "rdx.connector_handler.{0}".format(connection_type)
        )
        connection_handler_cls = getattr(connection_handler_module, "ConnectionHandler")
        self.connection_handler_cls_obj = connection_handler_cls()

    def produce_data(
        self,
        message: Union[dict, bytes],
        key: str = None,
        headers: dict = None,
        transaction_id: str = None,
        event_type: str = None,
        destination: Union[str, list] = None,
    ) -> None:
        produce_data_object = getattr(self.connection_handler_cls_obj, "produce_data")
        produce_data_object(
            key=key,
            value=message,
            headers=headers,
            transaction_id=transaction_id,
            event_type=event_type,
            destination=destination,
        )

    def app_settings(self):
        return env_reader.get()

    def consume_from_source(self, topic, partition, offset):
        produce_data_object = getattr(
            self.connection_handler_cls_obj, "consume_from_source"
        )
        return produce_data_object(topic=topic, partition=partition, offset=offset)

    def run(self):
        threading.Thread(
            target=getattr(self.connection_handler_cls_obj, "consume_data"),
        ).start()

    def consume_data(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            while not message_queue.empty() or message_queue.qsize != 0:
                func(message_queue.get(), *args, **kwargs)

        threading.Thread(target=wrapper).start()

    def consume_events(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            while not event_queue.empty() or event_queue.qsize != 0:
                func(event_queue.get(), *args, **kwargs)

        threading.Thread(target=wrapper).start()
