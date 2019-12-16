from core4.api.v1.request.queue.job import JobStream
from driverlicense.jobs.download import ScrapeFacts
from driverlicense.jobs.process import ProcessFiles
from driverlicense.jobs.extract import ExtractFacts
import datetime
import pandas as pd
from io import BytesIO
from bson.objectid import ObjectId


class AgofHandler(JobStream):
    author = "eha"

    async def post(self):
        # TODO get logger of both jobs?
        retrieve = await self.enqueue(ScrapeFacts)
        process = await self.enqueue(ProcessFiles)#, concurrent=True)
        return await super().get(process._id)

    async def get(self):
        start = self.get_argument("start", as_type=datetime.datetime, default=None)
        end = self.get_argument("end", as_type=datetime.datetime, default=None)
        if "download" in self.request.path:
            return await self.download_data(start, end)
        elif "analyse" in self.request.path:
            return await self.analyse_data(start, end)
        else:
            return self.render("template/driverlicense.html")

    async def download_data(self, start, end):
        self.target = self.config.driverlicense.collection.data
        cur = self.target.find()
        df = pd.DataFrame(await cur.to_list(None))
        if start and end:
            df.Date = pd.to_datetime(df.Date,format='%Y-%m-%d')
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
        self.temp = self.config.driverlicense.collection.temp
        if start and end:
            start = datetime.datetime.strftime(start, "%Y-%m-%d")
            end = datetime.datetime.strftime(end, "%Y-%m-%d")
        analyse = await self.enqueue(ExtractFacts, start=start, end=end)
        # jobId = str(analyse)
        jobId = await super().get(analyse._id)
        jobId = str(analyse._id)
        res = await self.temp.find_one({"_job_id": ObjectId(jobId)})
        result = [res['firstGraph'],res['secondGraph'],res['thirdGraph']]
        return result

    def filename(self, start, end):
        if start and end:
            import calendar
            mon1 = calendar.month_abbr[start.month] + str(start.year)
            mon2 = calendar.month_abbr[end.month] + str(end.year)
            filename = '_'.join(['AgofData',mon1, mon2])
        else:
            filename = "AgofData.xlsx"
        return filename

    def delete(self):
        pass

    def put(self):
        pass