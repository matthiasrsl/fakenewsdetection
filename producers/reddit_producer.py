import json
from datetime import datetime

import continuous_threading
import praw

from stream_producer import KafkaSourceStream


class KafkaRedditCommentStream(KafkaSourceStream):
    name = "reddit_comments"

    def __init__(self, bootstrap_servers, topic, client_id, client_secret, password, user_agent, username, subreddit_name):
        KafkaSourceStream.__init__(self, bootstrap_servers=bootstrap_servers, topic=topic)
        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            password=password,
            user_agent=user_agent,
            username=username,
        )
        self.subreddit = self.reddit.subreddit(subreddit_name)
        
    def format(self, comment):
        created_at_dt = datetime.fromtimestamp(comment.created_utc)
        comment_dict = {
            "source": "reddit",
            "type": "comment",
            "author_id": comment.author.name,
            "author_username": comment.author.name,
            "author_name": comment.author.name,
            "text": comment.body,
            "location": [None, None],
            "created_at": created_at_dt.isoformat() + ".000Z",
            "document_id": "redditcomment_" + comment.id
        }
        return json.dumps(comment_dict, ensure_ascii=False)

    def run(self):
        for comment in self.subreddit.stream.comments():
            self.send(self.format(comment))

    def start_stream(self):
        self.thread = continuous_threading.Thread(target=self.run)
        self.thread.start()

    def stop_stream(self):
        pass