from confluent_kafka import Producer, TopicPartition
import zlib, json, datetime, copy
from typing import Union

from ..kafka_consumer import KafkaConsumerHandler, encryption_handler


class KafkaProducerHandler(KafkaConsumerHandler):
    def __init__(self, **kwargs) -> None:
        KafkaConsumerHandler.__init__(self, **kwargs)

        self.producer = Producer(
            {
                "bootstrap.servers": kwargs["KAFKA_SERVERS"],
                "security.protocol": kwargs["KAFKA_SECURITY_PROTOCOL"],
                "sasl.username": kwargs["KAFKA_SASL_USERNAME"],
                "sasl.password": kwargs["KAFKA_SASL_PASSWORD"],
                "sasl.mechanism": kwargs["KAFKA_SASL_MECHANISM"],
                "message.timeout.ms": 10000,
                "compression.type": "snappy",
                "message.max.bytes": 5242880,
                "partitioner": "murmur2",
            }
        )

        consumer = self.create_consumer()

        self.topic_partition_offset_mapping = {}
        self.producer_topic_partition_mapping = kwargs[
            "PRODUCER_TOPIC_PARTITION_MAPPING"
        ]
        if self.producer_topic_partition_mapping:
            for (
                topic,
                partition_mapping,
            ) in self.producer_topic_partition_mapping.items():
                for partition in partition_mapping.keys():
                    _, high_offset = consumer.get_watermark_offsets(
                        TopicPartition(topic, int(partition))
                    )
                    if topic not in self.topic_partition_offset_mapping:
                        self.topic_partition_offset_mapping[topic] = {
                            partition: high_offset
                        }
                    else:
                        self.topic_partition_offset_mapping[topic][
                            partition
                        ] = high_offset

        self.consumer_producer_topic_partition_mapping = kwargs[
            "CONSUMER_PRODUCER_TOPIC_PARTITION_MAPPING"
        ]
        if self.consumer_producer_topic_partition_mapping:
            for (
                consumer_topic,
                consumer_partition_mapping,
            ) in self.consumer_producer_topic_partition_mapping.items():
                for consumer_partition in consumer_partition_mapping.keys():
                    for (
                        producer_topic,
                        producer_partitions,
                    ) in self.consumer_producer_topic_partition_mapping[consumer_topic][
                        consumer_partition
                    ].items():
                        if len(producer_partitions):
                            for partition in producer_partitions:
                                try:
                                    _, high_offset = consumer.get_watermark_offsets(
                                        TopicPartition(topic, int(partition))
                                    )
                                except Exception:
                                    high_offset = 0
                                if (
                                    producer_topic
                                    not in self.topic_partition_offset_mapping
                                ):
                                    self.topic_partition_offset_mapping[
                                        producer_topic
                                    ] = {partition: high_offset}
                                else:
                                    self.topic_partition_offset_mapping[producer_topic][
                                        partition
                                    ] = high_offset
                        else:
                            self.topic_partition_offset_mapping[producer_topic] = {
                                "0": 0
                            }

        consumer.close()

    def producer_thread(self, topic, partition, offset, data):
        data["value"]["data"]["{}_buffer_details".format(self.service_name)] = {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "topic": topic,
            "partition": int(partition) if partition else 0,
            "offset": offset,
        }

        if data["headers"] is None:
            data["headers"] = [(
                    "{}_produce_time".format(self.service_name),
                    datetime.datetime.now()
                    .strftime("%d-%m-%Y %H:%M:%S.%f")
                    .encode("utf-8"),
                )]
        else:
            data["headers"].append(
                (
                    "{}_produce_time".format(self.service_name),
                    datetime.datetime.now()
                    .strftime("%d-%m-%Y %H:%M:%S.%f")
                    .encode("utf-8"),
                )
            )
        data["value"] = json.dumps(data["value"])
        if partition:
            self.producer.produce(topic=topic, partition=int(partition), **data)
        else:
            self.producer.produce(topic=topic, **data)
        self.producer.flush()

    def produce_data(
        self,
        key,
        value,
        headers,
        transaction_id: str = None,
        event_type: str = None,
        destination: Union[str, list] = None,
    ):
        params = {
            "key": None,
            "value": {"source": self.service_name, "destination": None, "data": {}},
            "headers": None,
        }
        headers_tuples_array = []

        if key:
            params["key"] = key
        if value:
            if type(value) == bytes:
                params["value"]["data"] = {
                    "metadata": zlib.compress(value).decode("ISO-8859-1")
                }
            else:
                params["value"]["data"] = value

            data_keys = list(params["value"]["data"].keys())
            for key in data_keys:
                if key.find("_buffer_") != -1:
                    params["value"]["data"][
                        key.replace("id", "details")
                    ] = encryption_handler.decrypt(params["value"]["data"].pop(key))

        if headers and type(headers) == dict:
            for k, v in headers.items():
                headers_tuples_array.append(
                    (k, str(v) if type(v) != dict else json.dumps(v))
            )
        params["headers"] = headers_tuples_array

        if event_type:
            if event_type == "alert":
                params["value"]["destination"] = "alert_management"
                self.producer_thread(params["value"]["destination"], None, None, params)
            elif event_type == "intra_app_communication":
                params["value"]["data"].update({"app_local_id": self.app_local_id})
                self.producer_thread(
                    self.intra_app_communication_topic, None, None, params
                )
            elif destination:
                if event_type == "developed_apps":
                    for app in self.developed_apps:
                        if (
                            app["app_name"] in destination
                            and app["app_name"] != self.service_name
                        ):
                            params["value"]["destination"] = app["topic_name"]
                            params["value"]["data"][
                                "func_kwargs"
                            ] = encryption_handler.custom_data_encrypt(
                                encyption_key=app["secret_key"],
                                data=copy.deepcopy(
                                    params["value"]["data"]["func_kwargs"]
                                ),
                            )

                            self.producer_thread(
                                params["value"]["destination"], None, None, params
                            )

                else:
                    params["value"]["destination"] = destination
                    self.producer_thread(
                        params["value"]["destination"], None, None, params
                    )

        elif self.producer_topic_partition_mapping:
            for (
                topic,
                partition_mapping,
            ) in self.producer_topic_partition_mapping.items():
                for partition in partition_mapping.keys():
                    self.producer_thread(
                        topic,
                        partition,
                        self.topic_partition_offset_mapping[topic][partition],
                        params,
                    )
                    self.topic_partition_offset_mapping[topic][partition] += 1
        elif self.consumer_producer_topic_partition_mapping:
            transaction_data = encryption_handler.decrypt(transaction_id)
            topic_partition_mapping = self.consumer_producer_topic_partition_mapping[
                transaction_data["topic"]
            ][str(transaction_data["partition"])]

            for topic, partitions in topic_partition_mapping.items():
                if len(partitions):
                    for partition in partitions:
                        params["value"]["destination"] = topic
                        self.producer_thread(
                            topic,
                            partition,
                            self.topic_partition_offset_mapping[topic][partition],
                            params,
                        )
                        self.topic_partition_offset_mapping[topic][partition] += 1
                else:
                    params["value"]["destination"] = topic
                    self.producer_thread(
                        topic,
                        None,
                        self.topic_partition_offset_mapping[topic]["0"],
                        params,
                    )
                    self.topic_partition_offset_mapping[topic]["0"] += 1
