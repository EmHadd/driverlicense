import json

import pandas as pd

from core4.api.v1.application import CoreApiContainer
from core4.api.v1.request.main import CoreRequestHandler


class AsyncHandler(CoreRequestHandler):
    author = "mra"

    async def get(self):
        query = self.get_argument("query", as_type=dict, default={})
        collection = self.config.driverlicense.collection.data
        cursor = collection.find(query)
        data = await cursor.to_list(None)
        df = pd.DataFrame(data)
        self.render("xls-d.html", df=df, query=json.dumps(query))


class MyServer(CoreApiContainer):
    rules = [
        ("/xls", AsyncHandler)
    ]


if __name__ == '__main__':
    from core4.api.v1.tool.functool import serve

    serve(MyServer)
