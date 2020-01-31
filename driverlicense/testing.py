from core4.base.main import CoreBase
import pandas as pd
from io import BytesIO


class AgofTesting(CoreBase):

    def process(self, filename):
        basename = filename.split("/")[-1]
        # set the job source to the filename
        self.logger.info("extract [%s]", basename)
        # get the last version of the file form the database
        fh = self.gfs.get_last_version(filename)
        body = BytesIO(fh.read())
        body.seek(0)
        # create a dataframe from the retrieved data
        df = pd.read_excel(body, header=None)
        assert df.iloc[0, 0] == "Analyse"
        analyse = df.iloc[0, 1]
        assert df.iloc[1, 0] == "Grundgesamtheit"
        grundgesamtheit = df.iloc[1, 1]
        assert df.iloc[2, 0] == "Zeitraum"
        zeitraum = df.iloc[2, 1]
        assert df.iloc[3, 0] == "Vorfilter"
        vorfilter = df.iloc[3, 1]
        assert df.iloc[5, 0] == "Zielgruppe"
        zielgruppe = df.iloc[5, 1]
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
        return (d)

    def get_data(self):
        from gridfs import GridFS
        self.target = self.config.driverlicense.collection.data
        self.gfs = GridFS(self.target.connection[self.target.database])
        files = self.gfs.list()
        return files

    def process_data(self,n=10):
        fin_df = list()
        files = self.get_data()
        for i in range(0, n):
            df_processed = self.process(files[i])
            fin_df.append(df_processed)
        fin_df = pd.concat(fin_df, sort=True)
        return fin_df

if __name__ == "__main__":
    df = AgofTesting().process_data(n=15)
    print(df.columns)
