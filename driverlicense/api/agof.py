from core4.api.v1.request.queue.job import JobStream
from driverlicense.jobs.download import ScrapeFacts
from driverlicense.jobs.process import ProcessFiles
from driverlicense.jobs.extract import ExtractFacts
import datetime
import pandas as pd
from io import BytesIO
from bson.objectid import ObjectId

from core4.api.v1.request.main import CoreRequestHandler


class AgofHandler(JobStream):
    """
    The purpose of this API is to manage the Agof Jobs and retrieve results from MongoDB

    * ``GET /download`` - Download Agof Data as an Excel
    * ``GET /analyse`` - Analyse Agof Data and create input for FE
    * ``GET /analyse/<job_id>`` - Retrieve  results with a certain job id
    * ``POST /update`` - launch the ScrapeFacts and ProcessFiles jobs to update the data
    """

    author = "eha"

    async def post(self):
        """
          This api calls the ScrapeFacts and ProcessFiles Jobs

          Methods:
              POST /update

          Parameters:


          Returns:


          Raises:
             401: Unauthorized

          Examples:
            >>> import requests
            >>> login_url = "http://localhost:5001/core4/api/v1/login?username=admin&password=hans"
            >>> signin = requests.get(login_url)
            >>> token = signin.json()["data"]["token"]
            >>> headers = {"Authorization": "Bearer " + token}
            >>> driverlicense_api = "http://localhost:5001/driverlicense/api"
            >>> res = requests.post(driverlicense_api + "/update",
            >>>           headers=headers
            >>>           )
            <Response [200]>
            >>> for line in res.iter_lines():
            >>>     print(line)
            event: state
            data: {"killed_at":null,"locked":null,"priority":0,"finished_at":null,"_id":"5df8a9db6e188e7e949d1204","zombie_at":null,"name":"driverlicense.jobs.process.ProcessFiles","journal":false,"state":"pending","started_at":null,"prog":{"message":null,"value":null},"attempts_left":1,"trial":0,"removed_at":null,"runtime":null,"enqueued":{"hostname":"127.0.0.1","at":"2019-12-17T10:11:39.022000","username":"admin"},"attempts":1,"args":{"concurrent":true},"wall_at":null}
            event: state
            data: {"killed_at":null,"locked":{"hostname":"core4","worker":"worker@core4","at":"2019-12-17T10:11:40","heartbeat":"2019-12-17T10:11:40","pid":null},"priority":0,"finished_at":null,"_id":"5df8a9db6e188e7e949d1204","zombie_at":null,"name":"driverlicense.jobs.process.ProcessFiles","journal":false,"state":"running","started_at":"2019-12-17T10:11:40","prog":{"message":null,"value":null},"attempts_left":1,"trial":1,"removed_at":null,"runtime":null,"enqueued":{"hostname":"127.0.0.1","at":"2019-12-17T10:11:39.022000","username":"admin"},"attempts":1,"args":{"concurrent":true},"wall_at":null}
            event: log
            data: {"epoch":1576573901.239829,"hostname":"core4","identifier":"5df8a9db6e188e7e949d1204","_id":"5df8a9dd00529be4bb5654e3","qual_name":"driverlicense.jobs.process.ProcessFiles","level":"INFO","levelno":20,"created":"2019-12-17T10:11:41","message":"start execution","username":"eha"}
            event: log
            data: {"epoch":1576573901.245007,"hostname":"core4","identifier":"5df8a9db6e188e7e949d1204","_id":"5df8a9dd00529be4bb5654e5","qual_name":"driverlicense.jobs.process.ProcessFiles","level":"INFO","levelno":20,"created":"2019-12-17T10:11:41","message":"found [87] files to extract in [9] chunks","username":"eha"}
            event: state
            data: {"killed_at":null,"locked":{"hostname":"core4","worker":"worker@core4","at":"2019-12-17T10:11:40","heartbeat":"2019-12-17T10:11:40","pid":25737},"priority":0,"finished_at":null,"_id":"5df8a9db6e188e7e949d1204","zombie_at":null,"name":"driverlicense.jobs.process.ProcessFiles","journal":false,"state":"running","started_at":"2019-12-17T10:11:40","prog":{"message":null,"value":null},"attempts_left":1,"trial":1,"removed_at":null,"runtime":null,"enqueued":{"hostname":"127.0.0.1","at":"2019-12-17T10:11:39.022000","username":"admin"},"attempts":1,"args":{"concurrent":true},"wall_at":null}
            event: log
            data: {"epoch":1576573938.39726,"hostname":"core4","identifier":"5df8a9db6e188e7e949d1204","_id":"5df8aa0200529be4bb5654e7","qual_name":"driverlicense.jobs.process.ProcessFiles","level":"INFO","levelno":20,"created":"2019-12-17T10:12:18","message":"done execution with [complete] after [37] sec.","username":"eha"}
            event: state
            data: {"locked":null,"priority":0,"finished_at":"2019-12-17T10:12:17.852000","_id":"5df8a9db6e188e7e949d1204","zombie_at":null,"name":"driverlicense.jobs.process.ProcessFiles","journal":true,"state":"complete","started_at":"2019-12-17T10:11:40","prog":{"message":null,"value":null},"attempts_left":1,"trial":1,"removed_at":null,"killed_at":null,"enqueued":{"hostname":"127.0.0.1","at":"2019-12-17T10:11:39.022000","username":"admin"},"runtime":37.852,"attempts":1,"args":{"concurrent":true},"wall_at":null}
            event: close
            data: {}
          """
        retrieve = await self.enqueue(ScrapeFacts)
        process = await self.enqueue(ProcessFiles, concurrent=True)
        return await super().get(process._id)

    async def get(self, jobId=None):
        """
        This api call calls three different methods:
            * method to download the data
            * method to analys the data
            * method to retieve results from the database

        Methods:
           GET /download

        Parameters:
            start (str): start date of desired data
            end (str): end date of desired data

        Returns:
            Excel with agof data

        Raises:
          401: Unauthorized

        Examples:
            >>> import requests
            >>> login_url = "http://localhost:5001/core4/api/v1/login?username=admin&password=hans"
            >>> signin = requests.get(login_url)
            >>> token = signin.json()["data"]["token"]
            >>> headers = {"Authorization": "Bearer " + token}
            >>> driverlicense_api = "http://localhost:5001/driverlicense/api"
            >>> time = "?start=2017-09-01&end=2017-10-01"
            >>> res = requests.get(driverlicense_api + "/download/"+ time,
            >>>        headers=headers)
            >>> res
            <Response [200]>
            >>> res.headers
            {'Content-Length': '398354',
            'Access-Control-Allow-Headers': 'access-control-allow-origin,authorization,content-type',
            'Content-Disposition': 'attachment;
            filename=AgofData_Sep2017_Oct2017.xlsx',
            'Set-Cookie': 'token="2|1:0|10:1576576831|5:token|280:ZXlKMGVYQWlPaUpLVjFRaUxDSmhiR2NpT2lKSVV6VXhNaUo5LmV5SnVZVzFsSWpvaVlXUnRhVzRpTENKMGFXMWxjM1JoYlhBaU9qRTFOelkxTnpNeE5qTXVNRE01TkRRMUxDSmxlSEFpT2pFMU56WTJNRFUxTmpOOS5IMEpKeGdhWVh4WlJOLXh4aVpYeHdfUFhQOGVnd1FURU5sRXpRMmk2NmhlbjdTcEhJSC1SS2p2UGd5OVltQklIMV9SUzlscEFFVHBNYkVuY1BUdF96Zw==|971385027195227227662839543f68abfa61152a95452e37064a522d5a5ae014";
            expires=Thu, 16 Jan 2020 10:00:31 GMT;
            Path=/', 'Server': 'TornadoServer/6.0.3',
            'Etag': '"10b9fe0097e97f436bb1ff2d02161384eab21688"',
            'Date': 'Tue, 17 Dec 2019 10:00:31 GMT',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
            'Content-Type': 'application/octet-stream'}

        Methods:
           GET /analyse

        Parameters:
            start (str): start date of desired analysis
            end (str): end date of desired analysis

        Returns:
            id of the ExtractFacts job

        Raises:
          401: Unauthorized

        Examples:
            >>> res2 = requests.get(driverlicense_api + "/analyse",
            >>>           headers=headers
            >>>           )
            <Response [200]>
            >>> for line in res2.iter_lines():
            >>>     print(line)
            event: state
            data: {"attempts":1,"killed_at":null,"finished_at":null,"args":{"end":null,"start":null},"enqueued":{"at":"2019-12-17T10:18:07.639000","hostname":"127.0.0.1","username":"admin"},"state":"pending","started_at":null,"_id":"5df8ab5f6022e3acf7f798cb","zombie_at":null,"runtime":null,"prog":{"value":null,"message":null},"attempts_left":1,"name":"driverlicense.jobs.extract.ExtractFacts","removed_at":null,"locked":null,"trial":0,"wall_at":null,"journal":false,"priority":0}
            event: state
            data: {"attempts":1,"killed_at":null,"finished_at":null,"args":{"end":null,"start":null},"enqueued":{"at":"2019-12-17T10:18:07.639000","hostname":"127.0.0.1","username":"admin"},"state":"running","started_at":"2019-12-17T10:18:08","_id":"5df8ab5f6022e3acf7f798cb","zombie_at":null,"runtime":null,"prog":{"value":null,"message":null},"attempts_left":1,"name":"driverlicense.jobs.extract.ExtractFacts","removed_at":null,"locked":{"at":"2019-12-17T10:18:08","pid":null,"worker":"worker@core4","heartbeat":"2019-12-17T10:18:08","hostname":"core4"},"trial":1,"wall_at":null,"journal":false,"priority":0}
            event: log
            data: {"identifier":"5df8ab5f6022e3acf7f798cb","qual_name":"driverlicense.jobs.extract.ExtractFacts","message":"start execution","levelno":20,"epoch":1576574289.358141,"username":"eha","_id":"5df8ab61a7781f5200346bce","level":"INFO","created":"2019-12-17T10:18:09","hostname":"core4"}
            event: state
            data: {"attempts":1,"killed_at":null,"finished_at":null,"args":{"end":null,"start":null},"enqueued":{"at":"2019-12-17T10:18:07.639000","hostname":"127.0.0.1","username":"admin"},"state":"running","started_at":"2019-12-17T10:18:08","_id":"5df8ab5f6022e3acf7f798cb","zombie_at":null,"runtime":null,"prog":{"value":null,"message":null},"attempts_left":1,"name":"driverlicense.jobs.extract.ExtractFacts","removed_at":null,"locked":{"at":"2019-12-17T10:18:08","pid":27555,"worker":"worker@core4","heartbeat":"2019-12-17T10:18:08","hostname":"core4"},"trial":1,"wall_at":null,"journal":false,"priority":0}
            event: log
            data: {"identifier":"5df8ab5f6022e3acf7f798cb","qual_name":"driverlicense.jobs.extract.ExtractFacts","message":"retrieved data from MongoDB","levelno":20,"epoch":1576574290.337354,"username":"eha","_id":"5df8ab62a7781f5200346bd0","level":"INFO","created":"2019-12-17T10:18:10","hostname":"core4"}
            event: log
            data: {"identifier":"5df8ab5f6022e3acf7f798cb","qual_name":"driverlicense.jobs.extract.ExtractFacts","message":"Data created for the first graph","levelno":20,"epoch":1576574290.518309,"username":"eha","_id":"5df8ab62a7781f5200346bd1","level":"INFO","created":"2019-12-17T10:18:10","hostname":"core4"}
            event: log
            data: {"identifier":"5df8ab5f6022e3acf7f798cb","qual_name":"driverlicense.jobs.extract.ExtractFacts","message":"Data created for the second graph","levelno":20,"epoch":1576574290.55478,"username":"eha","_id":"5df8ab62a7781f5200346bd2","level":"INFO","created":"2019-12-17T10:18:10","hostname":"core4"}
            event: log
            data: {"identifier":"5df8ab5f6022e3acf7f798cb","qual_name":"driverlicense.jobs.extract.ExtractFacts","message":"Data created for the third graph","levelno":20,"epoch":1576574290.622501,"username":"eha","_id":"5df8ab62a7781f5200346bd3","level":"INFO","created":"2019-12-17T10:18:10","hostname":"core4"}
            event: log
            data: {"identifier":"5df8ab5f6022e3acf7f798cb","qual_name":"driverlicense.jobs.extract.ExtractFacts","message":"adding source basename of [5df8ab5f6022e3acf7f798cb]","levelno":20,"epoch":1576574290.623386,"username":"eha","_id":"5df8ab62a7781f5200346bd4","level":"INFO","created":"2019-12-17T10:18:10","hostname":"core4"}
            event: log
            data: {"identifier":"5df8ab5f6022e3acf7f798cb","qual_name":"driverlicense.jobs.extract.ExtractFacts","message":"inserted results","levelno":20,"epoch":1576574290.627754,"username":"eha","_id":"5df8ab62a7781f5200346bd6","level":"INFO","created":"2019-12-17T10:18:10","hostname":"core4"}
            event: state
            data: {"attempts":1,"killed_at":null,"finished_at":"2019-12-17T10:18:10.670000","args":{"end":null,"start":null},"enqueued":{"at":"2019-12-17T10:18:07.639000","hostname":"127.0.0.1","username":"admin"},"state":"complete","started_at":"2019-12-17T10:18:08","_id":"5df8ab5f6022e3acf7f798cb","zombie_at":null,"runtime":2.67,"prog":{"value":null,"message":null},"attempts_left":1,"name":"driverlicense.jobs.extract.ExtractFacts","removed_at":null,"locked":null,"trial":1,"wall_at":null,"journal":false,"priority":0}
            event: close
            data: {}

        Methods:
           GET /analyse/<job_id>

        Parameters:
            job_id: the id of the ExtractFacts Job that created the data in temp collection

        Returns:
            Data for the three required graphs

        Raises:
          401: Unauthorized

        Examples:
            >>> # continue example from above
            >>> jobId = "5df8ab5f6022e3acf7f798cb"
            >>> res3 = requests.get(driverlicense_api + "/analyse/"+ jobId
            >>>        headers=headers)
            >>>
            >>> res3.json()
            {u'_id': u'5df89f3bc264bd2f474b0abc',
            u'code': 200,
            u'data': [{u'2017-08-01': 67193.63000000006,
            u'2017-09-01': 66685.65000000008,
            u'2017-10-01': 69586.26999999981,
            u'2017-11-01': 68301.93999999994,
            u'2017-12-01': 65846.21000000015,
            u'2018-01-01': 71609.92999999996,
            u'2018-02-01': 65852.3500000001,
            u'2018-03-01': 70204.11999999991,
            u'2018-04-01': 67422.02999999981,
            u'2018-05-01': 67349.83999999998,
            u'2018-06-01': 64875.21000000012,
            u'2018-07-01': 64687.99000000012,
            u'2018-08-01': 65913.32000000002},
            {u'Digitales Gesamtangebot': 438328.5500000007,
            u'Mobiles Gesamtangebot': 265612.61000000034,
            u'Website Angebot': 171587.33000000022},
            [{u'Date': u'2017-08-01',
            u'Kontakte Mio': 33606.48000000002,
            u'Medientyp': u'Digitales Gesamtangebot'},
            {u'Date': u'2017-08-01',
            u'Kontakte Mio': 19551.44,
            u'Medientyp': u'Mobiles Gesamtangebot'},
            {u'Date': u'2017-08-01',
            u'Kontakte Mio': 14035.709999999992,
            u'Medientyp': u'Website Angebot'},
            {u'Date': u'2017-09-01',
            u'Kontakte Mio': 33352.72999999996,
            u'Medientyp': u'Digitales Gesamtangebot'},
            {u'Date': u'2017-09-01',
            u'Kontakte Mio': 19668.369999999977,
            u'Medientyp': u'Mobiles Gesamtangebot'},
            {u'Date': u'2017-09-01',
            u'Kontakte Mio': 13664.550000000001,
            u'Medientyp': u'Website Angebot'},
            {u'Date': u'2017-10-01',
            u'Kontakte Mio': 34805.39999999998,
            u'Medientyp': u'Digitales Gesamtangebot'},
            {u'Date': u'2017-10-01',
            u'Kontakte Mio': 20713.29,
            u'Medientyp': u'Mobiles Gesamtangebot'},
            {u'Date': u'2017-10-01',
            u'Kontakte Mio': 14067.57999999999,
            u'Medientyp': u'Website Angebot'},
            {u'Date': u'2017-11-01',
            u'Kontakte Mio': 34163.41000000004,
            u'Medientyp': u'Digitales Gesamtangebot'},
            {u'Date': u'2017-11-01',
            u'Kontakte Mio': 19803.679999999975,
            u'Medientyp': u'Mobiles Gesamtangebot'},
            {u'Date': u'2017-11-01',
            u'Kontakte Mio': 14334.849999999999,
            u'Medientyp': u'Website Angebot'},
            {u'Date': u'2017-12-01',
            u'Kontakte Mio': 32936.930000000044,
            u'Medientyp': u'Digitales Gesamtangebot'},
            {u'Date': u'2017-12-01',
            u'Kontakte Mio': 19385.19999999998,
            u'Medientyp': u'Mobiles Gesamtangebot'},
            {u'Date': u'2017-12-01',
            u'Kontakte Mio': 13524.079999999996,
            u'Medientyp': u'Website Angebot'},
            {u'Date': u'2018-01-01',
            u'Kontakte Mio': 35818.80999999998,
            u'Medientyp': u'Digitales Gesamtangebot'},
            {u'Date': u'2018-01-01',
            u'Kontakte Mio': 20679.709999999977,
            u'Medientyp': u'Mobiles Gesamtangebot'},
            {u'Date': u'2018-01-01',
            u'Kontakte Mio': 15111.409999999987,
            u'Medientyp': u'Website Angebot'},
            {u'Date': u'2018-02-01',
            u'Kontakte Mio': 32938.26999999998,
            u'Medientyp': u'Digitales Gesamtangebot'},
            {u'Date': u'2018-02-01',
            u'Kontakte Mio': 19586.799999999974,
            u'Medientyp': u'Mobiles Gesamtangebot'},
            {u'Date': u'2018-02-01',
            u'Kontakte Mio': 13327.280000000015,
            u'Medientyp': u'Website Angebot'},
            {u'Date': u'2018-03-01',
            u'Kontakte Mio': 35580.07,
            u'Medientyp': u'Digitales Gesamtangebot'},
            {u'Date': u'2018-03-01',
            u'Kontakte Mio': 20710.760000000024,
            u'Medientyp': u'Mobiles Gesamtangebot'},
            {u'Date': u'2018-03-01',
            u'Kontakte Mio': 13913.289999999983,
            u'Medientyp': u'Website Angebot'},
            {u'Date': u'2018-04-01',
            u'Kontakte Mio': 33711.1,
            u'Medientyp': u'Digitales Gesamtangebot'},
            {u'Date': u'2018-04-01',
            u'Kontakte Mio': 21428.789999999983,
            u'Medientyp': u'Mobiles Gesamtangebot'},
            {u'Date': u'2018-04-01',
            u'Kontakte Mio': 12282.139999999994,
            u'Medientyp': u'Website Angebot'},
            {u'Date': u'2018-05-01',
            u'Kontakte Mio': 33676.06999999991,
            u'Medientyp': u'Digitales Gesamtangebot'},
            {u'Date': u'2018-05-01',
            u'Kontakte Mio': 21595.860000000004,
            u'Medientyp': u'Mobiles Gesamtangebot'},
            {u'Date': u'2018-05-01',
            u'Kontakte Mio': 12077.90999999998,
            u'Medientyp': u'Website Angebot'},
            {u'Date': u'2018-06-01',
            u'Kontakte Mio': 32438.64000000003,
            u'Medientyp': u'Digitales Gesamtangebot'},
            {u'Date': u'2018-06-01',
            u'Kontakte Mio': 20826.11000000001,
            u'Medientyp': u'Mobiles Gesamtangebot'},
            {u'Date': u'2018-06-01',
            u'Kontakte Mio': 11610.459999999986,
            u'Medientyp': u'Website Angebot'},
            {u'Date': u'2018-07-01',
            u'Kontakte Mio': 32344.01000000001,
            u'Medientyp': u'Digitales Gesamtangebot'},
            {u'Date': u'2018-07-01',
            u'Kontakte Mio': 20563.07999999997,
            u'Medientyp': u'Mobiles Gesamtangebot'},
            {u'Date': u'2018-07-01',
            u'Kontakte Mio': 11780.899999999989,
            u'Medientyp': u'Website Angebot'},
            {u'Date': u'2018-08-01',
            u'Kontakte Mio': 32956.629999999925,
            u'Medientyp': u'Digitales Gesamtangebot'},
            {u'Date': u'2018-08-01',
            u'Kontakte Mio': 21099.52,
            u'Medientyp': u'Mobiles Gesamtangebot'},
            {u'Date': u'2018-08-01',
            u'Kontakte Mio': 11857.16999999999,
            u'Medientyp': u'Website Angebot'}]],
            u'message': u'OK',
            u'timestamp': u'2019-12-17T09:26:19.567154',
            u'version': u'driverlicense-0.0.4'}

           """
        start = self.get_argument("start", as_type=datetime.datetime,
                                  default=None)
        end = self.get_argument("end", as_type=datetime.datetime, default=None)
        if jobId:
            return self.reply(await self.get_data(jobId))
        if "download" in self.request.path:
            return await self.download_data(start, end)
        elif "analyse" in self.request.path:
            return await self.analyse_data(start, end)

    async def download_data(self, start, end):
        self.target = self.config.driverlicense.collection.data
        cur = self.target.find()
        df = pd.DataFrame(await cur.to_list(None))
        if start and end:
            df.Date = pd.to_datetime(df.Date, format='%Y-%m-%d')
            df2 = df[(df.Date >= start) & (df.Date <= end)]
        else:
            df2 = df.copy()
        filename = self.filename(start, end) + '.xlsx'
        self.set_header('Content-Disposition',
                        'attachment; filename=' + filename)
        self.set_header('Content-Type', 'application/octet-stream')

        dump = BytesIO()
        df2.to_excel(dump, index=False, encoding='utf-8')
        dump.seek(0)
        self.finish(dump.read())

    async def analyse_data(self, start, end):
        if start and end:
            start = datetime.datetime.strftime(start, "%Y-%m-%d")
            end = datetime.datetime.strftime(end, "%Y-%m-%d")
        analyse = await self.enqueue(ExtractFacts, start=start, end=end)
        return await super().get(analyse._id)

    async def get_data(self, jobId):
        self.temp = self.config.driverlicense.collection.temp
        res = await self.temp.find_one({"_job_id": ObjectId(jobId)})
        result = [res['firstGraph'], res['secondGraph'], res['thirdGraph']]
        return result

    def filename(self, start, end):
        if start and end:
            import calendar
            mon1 = calendar.month_abbr[start.month] + str(start.year)
            mon2 = calendar.month_abbr[end.month] + str(end.year)
            filename = '_'.join(['AgofData', mon1, mon2])
        else:
            filename = "AgofData.xlsx"
        return filename

    def delete(self):
        pass

    def put(self):
        pass

class AgofWidgetHandler(CoreRequestHandler):
    author = "mmr"
    title = "Agof Widget"
    tag = ["Agof", "analyse", "update"]

    async def get(self):
        return self.render("template/driverlicense.html")