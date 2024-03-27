import queue

from .env_reader import env_reader

message_queue = queue.Queue(
    maxsize=int(env_reader.get()["BUFFER_SIZE"])
    if "BUFFER_SIZE" in env_reader.get()
    else 1000
)

event_queue = queue.Queue(
    maxsize=int(env_reader.get()["BUFFER_SIZE"])
    if "BUFFER_SIZE" in env_reader.get()
    else 1000
)

queue_empty_exception = queue.Empty
