from core4.queue.job import CoreJob
from gridfs import GridFS
import pandas as pd
from io import BytesIO
import concurrent.futures
from core4.queue.helper.functool import enqueue


class ProcessFiles(CoreJob):
    author = "mra"
    max_parallel = 10

    def execute(self, test=False, threaded=False, concurrent=False, scope=None,
                chunk_size=10, *args, **kwargs):
        self.target = self.config.driverlicense.collection.data
        self.gfs = GridFS(self.target.connection[self.target.database])
        self.test =test
        if scope is None:
            files = self.gfs.list()
            if concurrent:
                chunks =[files[i:i + chunk_size]
                         for i in range(0, len(files), chunk_size)]
                self.logger.info("found [%d] files to extract in [%d] chunks",
                                 len(files), len(chunks))
                for launch in chunks:
                    enqueue(ProcessFiles, scope=launch, test=test)
            else:
                self.extract(files, threaded)
        else:
            self.extract(scope, threaded)

    def extract(self, files, threaded):
        # threaded
        if threaded:
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=8) as executor:
                executor.map(self.process, files)
        else:
            for f in files:
                self.process(f)
        n = self.target.count_documents({})
        self.logger.info("[%d] records with [%d] files", n, len(files))

    def process(self, filename):
        basename = filename.split("/")[-1]
        self.set_source(basename)
        self.logger.info("extract [%s]", basename)
        fh = self.gfs.get_last_version(filename)
        body = BytesIO(fh.read())
        body.seek(0)
        df = pd.read_excel(body, header=None)
        if self.test:
            return
        assert df.iloc[0, 0] == "Analyse"
        analyse = df.iloc[0, 1]
        assert df.iloc[1, 0] == "Grundgesamtheit"
        grundgesamtheit = df.iloc[1, 1]
        assert df.iloc[2, 0] == "Zeitraum"
        zeitraum = df.iloc[2, 1]
        assert df.iloc[3, 0] == "Vorfilter"
        vorfilter = df.iloc[3, 1]
        vorfilter_fallzahl = df.iloc[4, 1]
        assert df.iloc[5, 0] == "Zielgruppe"
        zielgruppe = df.iloc[5, 1]
        zielgruppe_fallzahl = df.iloc[6, 1]
        ln = 7
        while df.iloc[ln, 0] != "Basis":
            ln += 1
            if ln > 1000:
                raise RuntimeError("failed to identify start of data")
        d = df.iloc[ln:].copy()
        cols = list(df.iloc[ln - 1])
        cols[0] = "Titel"
        d.columns = ["" if pd.isnull(c)
                     else c.replace("\n", " ").replace(".", "") for c in cols]
        if "" in d.columns:
            d.drop([""], axis=1, inplace=True)
        d["Analyse"] = analyse
        d["Grundgesamtheit"] = grundgesamtheit
        d["Zeitraum"] = zeitraum
        d["Vorfilter"] = vorfilter
        d["Zielgruppe"] = zielgruppe
        doc = d.to_dict("rec")
        n = 0
        d = self.target.delete_many({"_src": basename})
        if d.deleted_count > 0:
            self.logger.info("reset [%d] records for [%s]", d.deleted_count,
                             basename)
        for rec in doc:
            nrec = {}
            for k, v in rec.items():
                if not pd.isnull(v):
                    nrec[k] = v
            self.target.insert_one(nrec)
            n += 1
        self.logger.info("inserted [%d] records for [%s]", n, basename)


if __name__ == '__main__':
    from core4.queue.helper.functool import execute
    execute(ProcessFiles, test=False)
