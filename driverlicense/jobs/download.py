from core4.queue.job import CoreJob
import requests
import re
from gridfs import GridFS
from core4.queue.helper.functool import enqueue
from driverlicense.jobs.process import ProcessFiles

url = "https://www.agof.de/service-downloads/downloadcenter/download-daily-digital-facts/"


class ScrapeFacts(CoreJob):
    author = "mra"
    schedule = "*/30 * * * * "
    def execute(self, test=False, *args, **kwargs):
        self.target = self.config.driverlicense.collection.data
        self.gfs = GridFS(self.target.connection[self.target.database])
        self.download(test)

    def download(self, test):
        rv = requests.get(url)
        body = rv.content.decode("utf-8")
        links = re.findall("href=[\"\'](.+?)[\"\']", body)
        xls_all = [href for href in links
               if href.endswith(".xls") or href.endswith(".xlsx")]
        xls = [filename for filename in xls_all if
               "Angebote_Ranking" in filename]
        self.logger.info("found [%d] xlsx files", len(xls))
        download = 0
        for link in xls:
            doc = self.gfs.find_one({"filename": link})
            if doc is None:
                self.logger.info("download [%s]", link)
                rv = requests.get(link)
                if not test:
                    self.gfs.put(rv.content, filename=link)
                download += 1
        self.logger.info("successfully retrieved [%d] of [%d] files",
                         download, len(xls))



if __name__ == '__main__':
    from core4.queue.helper.functool import execute
    execute(ScrapeFacts, test=False)
