import json

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
        data = json.loads(raw_data.decode("utf-8"))
        print(data)
        users = {user["id"]:user for user in data["includes"]["users"]}
        author = users[data["data"]["author_id"]]

        tweet = {
            "source": "twitter",
            "type": "tweet",
            "author_id": author["id"],
            "author_username": author["username"],
            "author_name": author["name"],
            "text": data["data"]["text"],
            "location": None,
            "created_at": data["data"]["created_at"],
            "tweet_id": "tweet_" + data["data"]["id"]
        }
        self.producer.send(self.topic, json.dumps(tweet, ensure_ascii=False).encode("utf-8"))

    def on_error(self, code):
        print(f"ERROR {code}")
        