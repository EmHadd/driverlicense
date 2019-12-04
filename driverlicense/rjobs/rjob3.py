from core4.queue.helper.job.r import CoreRJob


class RJob3(CoreRJob):
    author = "eha"

    def execute(self):
        self.data = self.config.driverlicense.collection.data
        r_session = self.get_rsession()
        self.func1(r_session)

    def func1(self, r):
        """
        In this function the connection to mongoDB happens in
        R with the help of mongolite package
        :param r: r session with required libraries
        :return:
        """
        url = self.config.driverlicense.mongo_url
        db = self.config.driverlicense.collection.data.database
        collection = self.config.driverlicense.collection.data.name
        r.source('script1.R')
        # r.debug("func1")
        res = r.func1(collection, db, url)
        self.logger.info('Dataframe size is %s', res.shape)
        return res


if __name__ == '__main__':
    from core4.queue.helper.functool import execute

    execute(RJob3)
