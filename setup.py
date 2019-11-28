from setuptools import find_packages, setup

import driverlicense

setup(
    name="driverlicense",
    author ="eha",
    version=driverlicense.__version__,
    packages=find_packages(exclude=['docs*', 'tests*']),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        "core4 @ git+https://github.com/plan-net/core4.git",
        "xlrd",
        "matplotlib",
        "feedparser",
        "spacy",
        "praw"
    ]
)
