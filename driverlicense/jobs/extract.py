from core4.queue.job import CoreJob
import pandas as pd
import numpy as np


url = "https://www.agof.de/service-downloads/downloadcenter/download-daily-digital-facts/"


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
        # create result dictionary for
        results = {}
        # first graph
        if start and end:
            df = df[(df.Date >= start & df.Date <= end)]
        df = df.replace(np.nan, 0)
        g = df.groupby(["Date"]).val.sum()
        results['firstGraph'] = g.to_dict()

        # second graph
        df_new = df[df.Medientyp != 0]
        g1 = df_new.groupby(["Medientyp"]).val.sum()
        results['secondGraph'] = g1.to_dict()

        # third graph
        df_new = df[df.Medientyp != 0]
        # Monthly contacts for each media group
        g2 = df_new.groupby(["Date", "Medientyp"]).val.sum().unstack()
        results['thirdGraph'] = g2.to_dict()
        self.set_source(str(self._id))
        self.temp.insert_one(results)

        self.logger.info("inserted results")



if __name__ == '__main__':
    from core4.queue.helper.functool import execute
    execute(ExtractFacts, test=False)
