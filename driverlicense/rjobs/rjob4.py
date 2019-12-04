import random

import pandas as pd

from core4.queue.helper.job.r import CoreRJob

class RJob(CoreRJob):
    author = "mra"

    def execute(self):
        df = pd.DataFrame([
            {
                "A": i,
                "B": random.randint(0, 5),
                "C": ("seg-1", "se"
                               "g-2", "seg-3")[random.randint(0, 2)]
            } for i in range(30)])
        ret = self.r(source="rjob4.r", df=df)
        self.logger.debug("shape 0 is %s", ret[0].shape)
        self.logger.debug("shape 1 is %s", ret[1].shape)

if __name__ == '__main__':
    from core4.queue.helper.functool import execute

    execute(RJob)
