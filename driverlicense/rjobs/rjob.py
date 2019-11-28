from core4.queue.job import CoreJob
import pandas as pd
from rpy2 import robjects as ro
from rpy2.robjects import pandas2ri
import os

import rpy2.robjects.packages as rpackages


from rpy2.robjects.vectors import StrVector

download_script = __file__
download_dir = os.path.dirname(__file__)
LIB_LOC = os.path.join(download_dir, 'R_lib','lib')

class RJob(CoreJob):

    author = "eha"

    def execute(self):
        pandas2ri.activate()
        R = ro.r
        df = pd.DataFrame({'x': [1,2,3,4,5],
                           'y': [2,1,3,5,4]})
        M = R.lm('y~x', data=df)
        #print(R.summary(M))
        R.plot(df.y)
        self.logger.info(R.summary(M).rx('coefficients'))
        R.gc()
        res = self.get_func()
        print(res.shape)

    def get_func(self):
        url = self.config.driverlicense.url
        db = self.config.driverlicense.db
        collection = self.config.driverlicense.coll_name
        r = ro.r
        rpackages.importr('mongolite',lib_loc= LIB_LOC)
        rpackages.importr('openssl', lib_loc=LIB_LOC)
        r.source('script1.R')
        res = r.func1(collection, db, url)

        return res


if __name__ == '__main__':
    from core4.queue.helper.functool import execute
    execute(RJob)