from core4.queue.job import CoreJob
from driverlicense.rjobs.rsession import get_rsession, as_rdf
import pandas as pd
import unidecode
import numpy as np
import os

import rpy2.robjects.packages as rpackages

download_script = __file__
download_dir = os.path.dirname(__file__)
LIB_LOC = os.path.join(download_dir, 'R_lib','lib')

class RJob(CoreJob):

    author = "eha"

    def execute(self):
        self.data = self.config.driverlicense.collection.data
        r_session = get_rsession()
        self.func1(r_session)
        #self.logger.info('imported and executed R func1')
        # self.func2(r_session)
        # self.logger.info('imported and executed R func2')

    def func1(self, r):
        """
        In this function the connection to mongoDB happens in
        R with the help of mongolite package
        :param r: r session with required libraries
        :return:
        """
        url = self.config.driverlicense['mongo_url']
        db = 'driverlicense'
        collection = 'xlsx'
        r.source('script1.R')
        # r.debug("func1")
        res = r.func1(collection, db, url)
        #self.logger.info('Dataframe size is %s', res.shape)
        return res

    def func2(self, r):
        """
        In this function R recieves the data as an R dataframe
        object using the function pandas2ri.py2ri
        :param r: r session with required libraries
        :return:
        """
        cur = self.data.find()
        df = pd.DataFrame(list(cur))
        df = df.replace(np.nan, 0)
        df['Monat'] = df['Monat'].dt.strftime(
            '%Y-%m-%d')
        df.columns = [self.remove_accents(c.replace("%"," percent")) for c in df.columns]
        for col in df.columns:
           df[col] = df[col].apply(self.remove_accents) if col in ['Grundgesamtheit', 'Titel', 'Analyse'] else df[col]
        #df = py_to_r(df)
        r.source('script1.R')
        res = r.func2()
        return res

    def remove_accents(self, string):
        return unidecode.unidecode(string)

if __name__ == '__main__':
    from core4.queue.helper.functool import execute
    execute(RJob)