from pytwitter import StreamApi


class MyStream(StreamApi):
    def on_tweet(self, tweet):
        print(f"{tweet.id}|{tweet.lang}|{tweet.text}")

    def on_error(self, code):
        print(f"ERROR {code}")


stream = MyStream(
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
