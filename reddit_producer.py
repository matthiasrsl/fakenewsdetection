import os

import praw
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

CLIENT_ID = os.environ['REDDIT_CLIENT_ID']
CLIENT_SECRET = os.environ['REDDIT_CLIENT_SECRET']
PASSWORD = os.environ['REDDIT_PASSWORD']
USERNAME = os.environ['REDDIT_USERNAME']
KAFKA_BROKER_IP = os.environ["KAFKA_BROKER_IP"]

reddit = praw.Reddit(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    password=PASSWORD,
    user_agent="fake_news_detector",
    username=USERNAME,
)

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_IP)
subreddit = reddit.subreddit("AskReddit")

comments = subreddit.stream.comments()
print(f"===== TYPE: {type(comments)}")

for comment in comments:
    producer.send("raw_tweets", comment.body.encode())

print("===== END")