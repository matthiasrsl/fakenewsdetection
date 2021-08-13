import json

from pytwitter import models as md

from kafka import KafkaProducer

class KafkaSourceStream:
    producers_registry = {}

    def __init_subclass__(cls):
        if cls.name in KafkaSourceStream.producers_registry:
            raise ValueError(
                "Producer name clash: Two or more stream producer "
                f"classes have the same name attribute '{cls.name}'. The name attribute "
                "of a stream producer class must be unique.\n"
                "Classes:\n"
                f"- {KafkaSourceStream.producers_registry[cls.name]}\n"
                f"- {cls}"
            )
        KafkaSourceStream.producers_registry[cls.name] = cls

    def __init__(self, bootstrap_servers, topic):
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        self.topic = topic
        print(f"Created producer '{self.name}'")

    def send(self, data):
        self.producer.send(self.topic, data.encode("utf-8"))

    def start_stream():
        pass

    def stop_stream():
        pass

