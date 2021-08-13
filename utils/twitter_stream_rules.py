import os

from pytwitter import StreamApi
from dotenv import load_dotenv


load_dotenv()
TWITTER_OAUTH_BEARER_TOKEN = os.environ["TWITTER_OAUTH_BEARER_TOKEN"]

stream = StreamApi(
    bearer_token=TWITTER_OAUTH_BEARER_TOKEN
)

rules_ids = [rule.id for rule in stream.get_rules().data]
rules_values = [rule.value for rule in stream.get_rules().data]

add_rules = {
    "add": [
        {"value": "(COVID OR covid OR coronavirus) lang:en -is:retweet -is:reply", "tag": "covid EN tweets"},
    ]
}

delete_rules = {
    "delete": {
        "ids": rules_ids
    }
}

for rule_value in rules_values:
    print(f" * Deleted rule:\n\t{rule_value}")

for rule in add_rules["add"]:
    print(f" * Added rule:\n\t{rule['value']}")

stream.manage_rules(delete_rules)
stream.manage_rules(add_rules)