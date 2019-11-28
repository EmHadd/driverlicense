# reference https://rpy2.readthedocs.io/en/version_2.7.x/robjects_rpackages.html

import os
import rpy2.robjects.packages as rpackages
from rpy2 import robjects as ro

from rpy2.robjects.vectors import StrVector

download_script = __file__
download_dir = os.path.dirname(__file__)
lib_loc = os.path.join(download_dir, 'lib')



# make sure you have this on linux
# sudo apt-get install libssl-dev libsasl2-dev
# sudo apt-get install libcurl4-openssl-dev "


# needed: openssl, swirl
def install_package(lib_loc, package_name):
    """

    :param lib_loc: location of the installation
    :param package_names: string of package name
    :return:
    """

    utils = rpackages.importr('utils')
    utils.chooseCRANmirror(ind=1)  # select the first mirror in the list
    utils.install_packages(package_name, lib=lib_loc)
    base = rpackages.importr('base')
    base._libPaths(ro.r.c(base._libPaths(), lib_loc))
    print(base._libPaths())

if __name__ == '__main__':
    package_name = 'mongolite'
    install_package(lib_loc, package_name)
    print(lib_loc)