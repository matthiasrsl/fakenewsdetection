from pytwitter import StreamApi
from kafka import KafkaProducer


class KafkaTwitterStream(StreamApi):
    def __init__(self, bootstrap_servers, topic, bearer_token):
        StreamApi.__init__(self, bearer_token=bearer_token)
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        self.topic = topic

    def on_tweet(self, tweet):
        self.producer.send(self.topic, tweet.text.encode("utf-8"))

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

# validate rules
#stream.manage_rules(rules=delete_rules, dry_run=False)

stream.manage_rules(rules=add_rules, dry_run=False)

# get tweets

#print(stream.get_rules())

stream.search_stream()
