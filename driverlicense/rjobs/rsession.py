from rpy2 import robjects as ro
import os
from rpy2.robjects.conversion import localconverter
from rpy2.robjects import pandas2ri
import rpy2.robjects.packages as rpackages

download_script = __file__
download_dir = os.path.dirname(__file__)
LIB_LOC = os.path.join(download_dir, 'R_lib','lib')


def get_rsession():
    """
    This function initiates R session with all the required
    packages (in this example mongolite, openssl

    :return:
    """
    # activating this functio allows an automotized transfer
    # from R dataframes to pandas dataframe
    pandas2ri.activate()
    r = ro.r
    rpackages.importr('base')
    rpackages.importr('mongolite', lib_loc=LIB_LOC)
    rpackages.importr('openssl', lib_loc=LIB_LOC)
    rpackages.importr('logging', lib_loc=LIB_LOC)

    return r

def as_rdf(df):
    """
    transform pandas data frame to  R dataframe
    :param df: pandas dataframe
    :return:
    """
    with localconverter(ro.default_converter + pandas2ri.converter):
        r_df = ro.conversion.py2rpy(df)
    return r_df