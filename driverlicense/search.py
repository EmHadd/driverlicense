import datetime
import random
import subprocess
import pandas as pd


def make_data(filename, count=1000000):
    segment = ["segment A", "segment B", "segment C", "segment D",
               "segment E"]
    t0 = datetime.datetime(2014, 1, 1)
    data = []
    for i in range(count):
        t0 += datetime.timedelta(hours=4)
        doc = {
            "timestamp": t0,
            "idx": i + 1,
            "real": random.random() * 100.,
            "value": random.randint(1, 20),
            "segment": segment[random.randint(0, 4)]
        }
        data.append(doc)
    df = pd.DataFrame(data)
    df.to_csv(filename)
    print(filename, df.shape)


def sort_file(*files):
    proc = []
    for f in list(files):
        cmd = 'sort -t , --key=3 -n ' \
              '"{filename}" > "{filename}.sorted.csv"'.format(filename=f)
        print(cmd)
        p = subprocess.Popen(cmd, shell=True)
        proc.append(p)
    for p in proc:
        print(p.wait())


if __name__ == '__main__':
    files = ["/tmp/file{}.csv".format(i) for i in range(1, 6)]
    #for f in files:
    #    make_data(f)
    sort_file(*files)
