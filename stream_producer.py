import json

from pytwitter import StreamApi
from pytwitter import models as md

from kafka import KafkaProducer


class KafkaSourceStream:
    def __init__(self, bootstrap_servers, topic):
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        self.topic = topic

    def format(self, raw_data):
        return raw_data

    def send(self, data):
        self.producer.send(self.topic, data.encode("utf-8"))

    def start_stream():
        pass

    def stop_stream():
        pass

class KafkaTwitterStream(KafkaSourceStream, StreamApi):
    def __init__(self, bootstrap_servers, topic, bearer_token):
        KafkaSourceStream.__init__(self, bootstrap_servers=bootstrap_servers, topic=topic)
        StreamApi.__init__(self, bearer_token=bearer_token)
        
    def format(self, raw_data):
        data = json.loads(raw_data.decode("utf-8"))
        users = {user["id"]:user for user in data["includes"]["users"]}
        author = users[data["data"]["author_id"]]
        tweet = {
            "source": "twitter",
            "type": "tweet",
            "author_id": author["id"],
            "author_username": author["username"],
            "author_name": author["name"],
            "text": data["data"]["text"],
            "location": [None, None],
            "created_at": data["data"]["created_at"],
            "document_id": "tweet_" + data["data"]["id"]
        }
        return json.dumps(tweet, ensure_ascii=False)

    def on_data(self, raw_data, return_json=False):
        """
        :param raw_data: Response data by twitter api.
        :param return_json:
        :return:
        """
        self.send(self.format(raw_data))

    def on_error(self, code):
        print(f"ERROR {code}")
        
    def start_stream(self):
        self.search_stream(
            tweet_fields=["text", "created_at",],
            expansions=["author_id",],
        )