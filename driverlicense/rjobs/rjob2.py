#
# from core4.queue.job import CoreJob
# from rpy2.robjects.packages import importr
# from rpy2 import robjects as ro
#
#
#
# class RJob2(CoreJob):
#
#     author = "eha"
#
#     def execute(self):
#         base = importr('base')
#         stats = importr('stats')
#         graphics = importr('graphics')
#
#         m = base.matrix(stats.rnorm(100), ncol = 5)
#         pca = stats.princomp(m)
#         ro.r.png("/tmp/rplot.png", width=350, height=350)
#         graphics.plot(pca, main = "Eigen values")
#         ro.r["dev.off"]()
#         ro.r.png("/tmp/rplot2.png", width=350, height=350)
#         stats.biplot(pca, main = "biplot")
#         ro.r["dev.off"]()
#         ro.r.gc()


if __name__ == '__main__':
    from core4.queue.helper.functool import execute
    execute(RJob2)