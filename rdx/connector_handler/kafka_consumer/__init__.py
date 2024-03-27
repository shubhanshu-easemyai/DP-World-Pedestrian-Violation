from confluent_kafka import Consumer, KafkaError, TopicPartition
import json, copy, random, string, threading, datetime, zlib

from ...core.encryption import EncyptionHandler
from ...core.message_queue import message_queue, event_queue

encryption_handler = EncyptionHandler()


class KafkaConsumerHandler:
    def __init__(self, **kwargs) -> None:
        self.consumer = None
        self.common_communication_topic = None
        self.intra_app_communication_topic = None
        self.event_sources = ["service_management", "widget_management", "frontend"]
        self.developed_apps = []

        self.service_name = kwargs["SERVICE_NAME"]
        self.service_category = kwargs["SERVICE_CATEGORY"]

        self.app_local_id = "{}_{}".format(
            self.service_name, self.generate_local_ap_id()
        )

        self.config = {
            "bootstrap.servers": kwargs["KAFKA_SERVERS"],
            "security.protocol": kwargs["KAFKA_SECURITY_PROTOCOL"],
            "sasl.username": kwargs["KAFKA_SASL_USERNAME"],
            "sasl.password": kwargs["KAFKA_SASL_PASSWORD"],
            "sasl.mechanism": kwargs["KAFKA_SASL_MECHANISM"],
            "auto.offset.reset": kwargs["KAFKA_AUTO_OFFSET_RESET"],
            "group.id": kwargs["KAFKA_CONSUMER_GROUP"],
            "session.timeout.ms": 10000,
            "enable.auto.commit": False,
        }
        if kwargs["CONSUMER_PRODUCER_TOPIC_PARTITION_MAPPING"]:
            self.consumer_producer_topic_partition_mapping = kwargs[
                "CONSUMER_PRODUCER_TOPIC_PARTITION_MAPPING"
            ]
            self.common_communication_topic = kwargs["KAFKA_COMMON_COMMUNICATION_TOPIC"]
            self.intra_app_communication_topic = kwargs[
                "KAFKA_INTRA_APP_COMMUNICATION_TOPIC"
            ]
            self.event_sources.extend(
                [self.common_communication_topic, self.intra_app_communication_topic]
            )

    def generate_local_ap_id(self, length: int = 7) -> str:
        return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))

    def create_consumer(self, load_latest_offset=False, load_custom_group=False):
        config = copy.deepcopy(self.config)
        if load_latest_offset:
            config["auto.offset.reset"] = "earliest"
        if load_custom_group:
            config["group.id"] = self.app_local_id
        return Consumer(**config)

    def consume_data(self, **kwargs):
        if self.common_communication_topic:
            threading.Thread(
                target=self.consumer_thread,
                kwargs={
                    "thread_name": self.common_communication_topic,
                    "thread_partition": None,
                },
            ).start()

        if self.intra_app_communication_topic:
            threading.Thread(
                target=self.consumer_thread,
                kwargs={
                    "thread_name": self.intra_app_communication_topic,
                    "thread_partition": None,
                },
            ).start()

        if self.consumer_producer_topic_partition_mapping:
            for topic_name in self.consumer_producer_topic_partition_mapping:
                if any(self.consumer_producer_topic_partition_mapping[topic_name]):
                    for partition in self.consumer_producer_topic_partition_mapping[
                        topic_name
                    ]:
                        threading.Thread(
                            target=self.consumer_thread,
                            kwargs={
                                "thread_name": topic_name,
                                "thread_partition": int(partition),
                            },
                        ).start()
                else:
                    threading.Thread(
                        target=self.consumer_thread,
                        kwargs={
                            "thread_name": topic_name,
                            "thread_partition": None,
                        },
                    ).start()

    def consume_from_source(self, topic, partition, offset):
        consumer = self.create_consumer()

        partition = TopicPartition(
            topic=topic,
            partition=int(partition),
            offset=int(offset),
        )

        consumer.assign([partition])
        while True:
            message = consumer.poll()
            # data = json.loads(message.value())
            data = message.value()
            # transaction_data = {
            #     "timestamp": datetime.datetime.timestamp(
            #         datetime.datetime.utcnow()
            #     ),
            #     "topic": message.topic(),
            #     "partition": message.partition(),
            #     "offset": message.offset(),
            #     "source": data.pop("source"),
            #     "destination": data.pop("destination"),
            # }
            # if message.offset() == source_buffer_details["offset"]:
            # if "metadata" in data["data"]:
            #     try:
            #         data["data"]["metadata"] = zlib.decompress(
            #             data["data"]["metadata"].encode("ISO-8859-1")
            #         )
            #     except Exception:
            #         pass
            # else:
            #     print("*** IMAGE NOT FOUND ***")
            #     data["data"] = {"metadata":None}

            # data_keys = list(data["data"].keys())
            # for key in data_keys:
            #     if key.find("_buffer_") != -1:
            #         data["data"][
            #             key.replace("details", "id")
            #         ] = encryption_handler.encrypt(data["data"].pop(key))

            # if message.key():
            #     data.update({"key": message.key().decode("utf-8")})

            # data["headers"] = {}
            # if message.headers():
            #     for header in message.headers():
            #         data["headers"].update({header[0]: header[1].decode()})

            # data["transaction_id"] = encryption_handler.encrypt(
            #     transaction_data
            # )
            
            consumer.close()
            return data

    def consumer_thread(self, **kwargs):
        if kwargs["thread_name"] == self.intra_app_communication_topic:
            consumer_object = self.create_consumer(
                load_latest_offset=True, load_custom_group=True
            )
        else:
            consumer_object = self.create_consumer(load_latest_offset=True)
    
        # if kwargs["thread_name"] == "person_tresspassing":
        #     kwargs["thread_partition"] = 0

        if kwargs["thread_partition"] is None:
            consumer_object.subscribe([kwargs["thread_name"]])
        else:
            _, high_offset = consumer_object.get_watermark_offsets(
                TopicPartition(kwargs["thread_name"], int(kwargs["thread_partition"]))
            )
            consumer_object.assign(
                [
                    TopicPartition(
                        kwargs["thread_name"],
                        int(kwargs["thread_partition"]),
                        high_offset,
                    )
                ]
            )
        if consumer_object:
            try:
                while True:
                    message = consumer_object.poll(0.01)
                    # print(message)
                    if message is None:
                        continue
                    elif message.error():
                        if message.error().code() == KafkaError._PARTITION_EOF:
                            print("Error occurred: End of partition")
                        else:
                            print("Error occurred: {}".format(message.error().str()))
                    else:
                        data = json.loads(message.value())

                        if "app_local_id" in data["data"] and data["data"]["app_local_id"] == self.app_local_id:
                            continue

                        transaction_data = {
                            "timestamp": datetime.datetime.timestamp(
                                datetime.datetime.utcnow()
                            ),
                            "topic": message.topic(),
                            "partition": message.partition(),
                            "offset": message.offset(),
                            "source": data.pop("source"),
                            "destination": data.pop("destination"),
                        }

                        if "metadata" in data["data"]:
                            try:
                                data["data"]["metadata"] = zlib.decompress(
                                    data["data"]["metadata"].encode("ISO-8859-1")
                                )
                            except Exception:
                                pass

                        data_keys = list(data["data"].keys())
                        for key in data_keys:
                            if key.find("_buffer_") != -1:
                                data["data"][
                                    key.replace("details", "id")
                                ] = encryption_handler.encrypt(data["data"].pop(key))

                        if message.key():
                            data.update({"key": message.key().decode("utf-8")})

                        data["headers"] = {
                            "{}_consume_time".format(
                                self.service_name
                            ): datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S.%f")
                        }
                        if message.headers():
                            for header in message.headers():
                                data["headers"].update({header[0]: header[1].decode()})

                        data["transaction_id"] = encryption_handler.encrypt(
                            transaction_data
                        )

                        if (
                            kwargs["thread_name"] in self.event_sources
                            or transaction_data["source"] in self.event_sources
                        ):
                            if (
                                "encryption" in data["data"]["func_kwargs"]
                                and data["data"]["func_kwargs"]["encryption"]
                            ):
                                decrypted_data = encryption_handler.decrypt(
                                    data["data"]["func_kwargs"]["data"]
                                )
                                if decrypted_data:
                                    if data["data"]["task_name"] != "developed_apps":
                                        data["data"]["func_kwargs"][
                                            "data"
                                        ] = copy.deepcopy(decrypted_data)
                                        event_queue.put(copy.deepcopy(data))
                                    else:
                                        self.developed_apps = copy.deepcopy(
                                            decrypted_data
                                        )
                            else:
                                event_queue.put(copy.deepcopy(data))
                        else:
                            message_queue.put(copy.deepcopy(data))

                        del data
                        del transaction_data

            except Exception as e:
                print(data)
                print("Error occurred: {}".format(e))
        else:
            print("consumer topics are not present")
