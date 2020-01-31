import pytest
from driverlicense.api.server import MyServer

import tornado
import urllib.parse
import tornado.gen
import tornado.ioloop
import tornado.simple_httpclient
import tornado.web
import time
import threading

import json
import logging
import os
import urllib.parse

import pymongo

from tornado import httpclient
from contextlib import closing
from http.cookies import SimpleCookie

import core4.logger.mixin
import core4.queue.main
from core4.api.v1.server import CoreApiServer
from core4.api.v1.tool.serve import CoreApiServerTool

RESOURCES_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../driverlicense'))

MONGO_URL = 'mongodb://core:654321@testmongo:27017'
MONGO_DATABASE = "driverlicensetest"


@pytest.fixture(autouse=True)
def setup(tmpdir):
    logging.shutdown()
    core4.logger.mixin.CoreLoggerMixin.completed = False
    os.environ["CORE4_CONFIG"] = os.path.join(RESOURCES_DIR, 'driverlicense.yaml')
    os.environ["CORE4_OPTION_folder__root"] = str(tmpdir)
    os.environ["CORE4_OPTION_DEFAULT__mongo_url"] = MONGO_URL
    os.environ["CORE4_OPTION_DEFAULT__mongo_database"] = MONGO_DATABASE
    os.environ["CORE4_OPTION_logging__mongodb"] = "DEBUG"
    os.environ["CORE4_OPTION_api__token__expiration"] = "!!int 60"
    os.environ["CORE4_OPTION_api__setting__debug"] = "!!bool True"
    os.environ["CORE4_OPTION_api__setting__cookie_secret"] = "blabla"
    os.environ["CORE4_OPTION_worker__min_free_ram"] = "!!int 32"
    conn = pymongo.MongoClient(MONGO_URL)
    conn.drop_database(MONGO_DATABASE)
    core4.logger.mixin.logon()
    yield
    conn.drop_database(MONGO_DATABASE)
    for i, j in core4.service.setup.CoreSetup.__dict__.items():
        if callable(j):
            if "has_run" in j.__dict__:
                j.has_run = False
    core4.util.tool.Singleton._instances = {}
    dels = []
    for k in os.environ:
        if k.startswith('CORE4_'):
            dels.append(k)
    for k in dels:
        del os.environ[k]


@pytest.fixture
def mongodb():
    return pymongo.MongoClient(MONGO_URL)[MONGO_DATABASE]


class WorkerHelper:
    def __init__(self):
        self.queue = core4.queue.main.CoreQueue()
        self.pool = []
        self.worker = []

    def start(self, num=3):
        for i in range(0, num):
            worker = core4.queue.worker.CoreWorker(
                name="worker-{}".format(i + 1))
            self.worker.append(worker)
            t = threading.Thread(target=worker.start, args=())
            self.pool.append(t)
        for t in self.pool:
            t.start()
        print("THREAD ends now")

    def stop(self):
        for worker in self.worker:
            worker.exit = True
        for t in self.pool:
            t.join()

    def wait_queue(self):
        while self.queue.config.sys.queue.count_documents({}) > 0:
            time.sleep(1)
        self.stop()


class HTTPTestServerClient(tornado.simple_httpclient.SimpleAsyncHTTPClient):
    def initialize(self, *, http_server=None, port=None):
        super().initialize()
        self._http_server = http_server
        self._http_port = port
        self.token = None
        self.admin_token = None

    async def login(self, username="admin", password="hans"):
        self.token = None
        resp = await self.get(
            "/core4/api/v1/login?username={}&password={}".format(
                username, password
            ))
        assert resp.code == 200
        self.token = resp.json()["data"]["token"]
        if username == "admin":
            self.admin_token = self.token

    def set_admin(self):
        self.token = self.admin_token

    def post(self, path, body="", **kwargs):
        return self._fetch("POST", path=path, body=body, **kwargs)

    def put(self, path, body="", **kwargs):
        return self._fetch("PUT", path=path, body=body, **kwargs)

    def get(self, path, **kwargs):
        return self._fetch("GET", path, **kwargs)

    def delete(self, path, **kwargs):
        return self._fetch("DELETE", path=path, **kwargs)

    async def _fetch(self, method, path, **kwargs):
        headers = kwargs.get("headers", {})
        if self.token:
            headers["Authorization"] = "bearer " + self.token
        kwargs["headers"] = headers
        if "body" in kwargs:
            body = kwargs["body"]
            if isinstance(body, dict):
                body = json.dumps(body)
            kwargs["body"] = body
        if "json" in kwargs:
            body = kwargs.pop("json")
            body = json.dumps(body)
            headers["Content-Type"] = "application/json"
            kwargs["body"] = body
        req = httpclient.HTTPRequest(
            self.get_url(path), method=method, **kwargs)
        resp = await super().fetch(req, raise_error=False)
        return self._postproc(resp)

    def _postproc(self, response):
        def _json():
            return json.loads(response.body.decode("utf-8"))

        def _cookie():
            cookie = SimpleCookie()
            s = response.headers.get("set-cookie")
            cookie.load(s)
            return cookie

        response.json = _json
        response.cookie = _cookie
        response.ok = response.code == 200
        return response

    def get_url(self, path):
        p, *q = path.split("?")
        elems = urllib.parse.parse_qs("?".join(q))
        if q:
            p += "?" + urllib.parse.urlencode(elems, doseq=True)
        url = "http://127.0.0.1:%s%s" % (self._http_port, p)
        return url


@pytest.yield_fixture
def api():
    yield from run(
        CoreApiServer,
        MyServer
    )


@pytest.fixture
def worker():
    return WorkerHelper()


def run(*app):
    loop = tornado.ioloop.IOLoop().current()
    http_server_port = tornado.testing.bind_unused_port()[1]
    serve = CoreApiServerTool()
    server = serve.create_routes(*app, port=http_server_port, api=False)
    serve.init_callback()

    with closing(HTTPTestServerClient(http_server=server,
                                      port=http_server_port)) as client:
        yield client

    serve.unregister()
    server.stop()

    if hasattr(server, "close_all_connections"):
        loop.run_sync(
            server.close_all_connections
        )


async def test_core4_server(api):
    resp = await api.get(
        '/core4/api/v1/login?username=admin&password=hans')
    assert resp.code == 200


async def test_get_results(api):
    await api.login()
    res_id = "5e316e750c8153480ff5801f"
    res = await api.get("/driverlicense/api/analyse/" + res_id)
    assert 200 == res.code

@pytest.mark.api_job
async def test_get_results3(api, worker):
    import ast
    import re
    await api.login()
    worker.start()
    time.sleep(5)
    res = await api.get("/driverlicense/api/analyse/")

    assert 200 == res.code
    dict_res = ast.literal_eval("{" + re.findall("\"identifier\":\"[a-zA-Z\d]+\"",
                                      res.body.decode("utf-8"))[0] + "}")
    res_id = dict_res['identifier']
    res2 = await api.get("/driverlicense/api/analyse/" + res_id)
    assert 200 == res2.code
    worker.stop()

    data = {
        "Mobiles Gesamtangebot" : 334026.96,
        "Digitales Gesamtangebot" : 548501.41,
        "Website Angebot" : 203452.67
    }

    res_data = ast.literal_eval(res2.body.decode("utf-8"))["data"][1]
    for i in data:
        assert data[i] == pytest.approx(res_data[i])

