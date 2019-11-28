from core4.queue.job import CoreJob
import pandas as pd
from rpy2 import robjects as ro
from rpy2.robjects import pandas2ri

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


if __name__ == '__main__':
    from core4.queue.helper.functool import execute
    execute(RJob)