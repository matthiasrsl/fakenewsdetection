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


stream = KafkaTwitterStream(
    "localhost:9092",
    "raw_tweets",
    bearer_token="AAAAAAAAAAAAAAAAAAAAANUlPgEAAAAA3n%2B61l0gpELzltUuU68CusMSFy4%3D6jmJKTf0VRzglbq4KgQQfNTGaUqAueQ06WtDUcoTLi4jiq4ajI"
)

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


stream.manage_rules(rules=add_rules, dry_run=False)


stream.search_stream(
    tweet_fields=["text", "created_at"],
    expansions=["author_id"])
'''user_fields=["name", "screen_name"],
    return_json=True
)
'''