from core4.api.v1.application import CoreApiContainer
from driverlicense.api.agof import AgofHandler


class MyServer(CoreApiContainer):
    root = "/driverlicense/api/"
    rules = [
        ("/update", AgofHandler),
        ("/download", AgofHandler),
        ("/analyse", AgofHandler),
        ("/analyse/(.*)", AgofHandler),
        ("/html", AgofHandler)
    ]


if __name__ == '__main__':
    from core4.api.v1.tool.functool import serve
    serve(MyServer)