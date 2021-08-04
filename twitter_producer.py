from pytwitter import StreamApi
from pytwitter import models as md

from kafka import KafkaProducer


class KafkaTwitterStream(StreamApi):
    def __init__(self, bootstrap_servers, topic, bearer_token):
        StreamApi.__init__(self, bearer_token=bearer_token)
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        self.topic = topic

    def on_data(self, raw_data, return_json=False):
        """
        :param raw_data: Response data by twitter api.
        :param return_json:
        :return:
        """
        self.producer.send(self.topic, raw_data)

    def on_error(self, code):
        print(f"ERROR {code}")




add_rules = {
    "add": [
        {"value": "(COVID OR covid OR coronavirus) lang:en -is:retweet -is:reply", "tag": "covid EN tweets"},
    ]
}

delete_rules = {
    "delete": {
        "ids": [
            
        ]
    }
}