from core4.api.v1.application import CoreApiContainer
from driverlicense.api.agof import AgofHandler



class MyServer(CoreApiContainer):
    root = "/driverlicense/api/"
    rules = [
        ("/update", AgofHandler),
        ("/analyse", AgofHandler),
        ("/download", AgofHandler),
        ("/html", AgofHandler)
    ]


if __name__ == '__main__':
    from core4.api.v1.tool.functool import serve
    serve(MyServer)