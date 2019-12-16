from core4.queue.job import CoreJob
import pandas as pd
import numpy as np
from datetime import datetime

class ExtractFacts(CoreJob):
    author = "mra"
    schedule = "*/30 * * * * "

    def execute(self, start=None, end=None, *args, **kwargs):
        """
        :param test: control, if test don't write data to mongoDB
        :param args:
        :param kwargs:
        :return:
        """
        self.target = self.config.driverlicense.collection.data
        self.temp = self.config.driverlicense.collection.temp
        self.analyse(start, end)

    def analyse(self, start, end):
        """
        This function gets the data from the database,
        creates a dataframe for the analysis and retutns
        the final resutls
        :param test:
        :return:
        """
        cur = self.target.find()
        df = pd.DataFrame(list(cur))
        self.logger.info("retrieved data from MongoDB")
        # create result dictionary for
        results = {}
        # first graph
        if start and end:
            start = datetime.strptime(start, "%Y-%m-%d")
            end = datetime.strptime(end, "%Y-%m-%d")
            df.Date = pd.to_datetime(df.Date, format="%Y-%m-%d")
            df = df[(df.Date >= start) & (df.Date <= end)]
            df.Date = df.Date.apply(lambda x: datetime.strftime(x, "%Y-%m-%d"))
        df['Kontakte Mio'].replace("--", None, inplace=True)
        df = df.replace(np.nan, 0)
        g = df.groupby(["Date"])['Kontakte Mio'].agg('sum')
        results['firstGraph'] = g.to_dict()
        self.logger.info("Data created for the first graph")

        # second graph
        df_new = df[df.Medientyp != 0]
        g1 = df_new.groupby(["Medientyp"])['Kontakte Mio'].agg('sum')
        results['secondGraph'] = g1.to_dict()
        self.logger.info("Data created for the second graph")

        # third graph
        df_new = df[df.Medientyp != 0]
        # Monthly contacts for each media group
        g2 = df_new.groupby(["Date", "Medientyp"])['Kontakte Mio'].agg('sum')
        results['thirdGraph'] = g2.reset_index().to_dict('rec')
        self.logger.info("Data created for the third graph")
        self.set_source(str(self._id))
        self.temp.insert_one(results)
        self.logger.info("inserted results")



if __name__ == '__main__':
    from core4.queue.helper.functool import execute
    execute(ExtractFacts, test=False)
