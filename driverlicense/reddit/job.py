from core4.queue.job import CoreJob
import urllib
import re
import os
import praw
import time
import datetime
from core4.queue.helper.functool import enqueue


class RedditCollector(CoreJob):
    author = 'mkr'
    schedule = "*/30 * * * * "

    def execute(self, jobs=2, subreddit='europe', **kwargs):

        reddit = praw.Reddit(client_id=self.class_config.client_id,
                             client_secret=self.class_config.secret,
                             user_agent='core4os')

        images = []

        for submission in reddit.subreddit(subreddit).hot(limit=100):
            if re.match(r".*(jpg|gif|png)$", submission.url):
                images.append(
                    (submission.url, submission.title, submission.score))

        self.class_config.reddit_coll.insert_one(
            {"_id": "_control_" + subreddit, "images": images})

        for i in range(0, jobs):
            enqueue(RedditDownloader, master=str(self._id),
                    id=subreddit + str(i), subreddit=subreddit)
            time.sleep(1)


class RedditDownloader(CoreJob):
    author = 'mkr'

    def execute(self, subreddit='europe', **kwargs):

        self.set_source(".")

        path = "/tmp/reddit/" + subreddit + "/" + \
               datetime.datetime.today().strftime("%Y-%m-%d")

        if not os.path.exists(path):
            os.makedirs(path)

        doc = self.config.driverlicense.reddit.job.RedditCollector.reddit_coll \
            .find_one({"_id": "_control_" + subreddit})
        if doc and "images" in doc.keys():
            init_length = len(doc["images"])
        while doc and len(doc["images"]) > 0:
            img = doc["images"][0]
            cur = self.config.driverlicense.reddit.job.RedditCollector \
                .reddit_coll.update_one({"_id": "_control_" + subreddit},
                                        {"$pull": {"images": img}})

            # success, got image to download
            if cur.modified_count != 0:
                self.logger.info("downloading image [%s]", img[0])
                # print("downloading image [%s]", img[0])
                file = path + "/" + str(img[1]).replace("/", "")[0:100]
                try:
                    urllib.request.urlretrieve(img[0], file)
                except urllib.error.URLError:
                    self.logger.warning("skippe image [%s]", img[0])
                self.progress((init_length - len(doc["images"])) / init_length)
                del doc["images"][0]

            # img already taken
            else:
                # update jobs work
                doc = self.config.driverlicense.reddit.job.RedditCollector \
                    .reddit_coll.find_one({"_id": "_control_"
                                                  + subreddit})
        self.progress(1)
        self.logger.info("Nothing to do, no more images")
        try:
            self.config.driverlicense.reddit.job.RedditCollector.reddit_coll\
                .remove({"_id": "_control_" + subreddit})
        except BaseException:
            pass


if __name__ == '__main__':
    import core4.queue.helper.functool

    core4.queue.helper.functool.execute(RedditCollector)
