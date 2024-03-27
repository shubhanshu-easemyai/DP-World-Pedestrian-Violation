import copy

from ..core.env_reader import env_reader
from .kafka_producer import KafkaProducerHandler


class ConnectionHandler(KafkaProducerHandler):
    def __init__(self) -> None:
        self.configurations = {
            "KAFKA_SERVERS": None,
            "KAFKA_SECURITY_PROTOCOL": None,
            "KAFKA_SASL_USERNAME": None,
            "KAFKA_SASL_PASSWORD": None,
            "KAFKA_SASL_MECHANISM": None,
            "KAFKA_AUTO_OFFSET_RESET": None,
            "KAFKA_CONSUMER_GROUP": None,
            "KAFKA_COMMON_COMMUNICATION_TOPIC": None,
            "KAFKA_INTRA_APP_COMMUNICATION_TOPIC": None,
            "PRODUCER_TOPIC_PARTITION_MAPPING": None,
            "CONSUMER_PRODUCER_TOPIC_PARTITION_MAPPING": None,
        }
        self.update_configurations()
        KafkaProducerHandler.__init__(self, **self.configurations)

    def update_configurations(self):
        envs = copy.deepcopy(env_reader.get())
        for key, val in envs.items():
            if key in self.configurations:
                self.configurations[key] = val
                env_reader.remove(env_name=key)

        self.configurations["SERVICE_NAME"] = copy.deepcopy(envs["SERVICE_NAME"])
        self.configurations["SERVICE_CATEGORY"] = copy.deepcopy(
            envs["SERVICE_CATEGORY"]
        )
        del envs

    def consume_data(self, **kwargs):
        return super().consume_data(**kwargs)

    def produce_data(
        self,
        key,
        value,
        headers: dict = {},
        transaction_id: str = None,
        event_type: str = None,
        destination: str = None,
    ):
        return super().produce_data(
            key, value, headers, transaction_id, event_type, destination
        )

    # def produce_data(
    #     self,
    #     key,
    #     value,
    #     headers: dict = None,
    #     transaction_id: str = None,
    #     event_type: str = None,
    #     destination: str = None,
    # ):
    #     if headers is None:
    #         headers = {}
    #     return super().produce_data(
    #         key, value, headers, transaction_id, event_type, destination
    #     )

