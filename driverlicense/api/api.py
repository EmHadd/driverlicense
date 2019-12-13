from core4.api.v1.request.main import CoreRequestHandler
from core4.api.v1.application import CoreApiContainer

class MyHandler(CoreRequestHandler):
    author = "mra"

    def post(self):
        import pandas as pd
        df = pd.DataFrame({"spalte 1": [1,2,3], "spalte 2": ['a', 'b', 'c']})
        self.reply(df)

    def get(self):
        age = self.get_argument("alter", as_type=int, default=18)
        js = self.get_argument("query", as_type=dict, default=None)
        if js.get("create", False):
            self.reply("NONONO")
        else:
            self.render("test.html", variable1 = "guten Tag", age=age, query=js)


class MyServer(CoreApiContainer):

    rules = [
        ("/hello", MyHandler),
        ("/hallo", MyHandler)
    ]


if __name__ == '__main__':
    from core4.api.v1.tool.functool import serve
    serve(MyServer)
