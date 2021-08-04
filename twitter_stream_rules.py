from pytwitter import StreamApi

stream = StreamApi(
    bearer_token="AAAAAAAAAAAAAAAAAAAAANUlPgEAAAAA3n%2B61l0gpELzltUuU68CusMSFy4%3D6jmJKTf0VRzglbq4KgQQfNTGaUqAueQ06WtDUcoTLi4jiq4ajI"
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