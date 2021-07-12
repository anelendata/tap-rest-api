#!/usr/bin/env python
from setuptools import setup

VERSION = "0.2.6"

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="tap-rest-api",
    version=VERSION,
    description="Singer.io tap for extracting data from any REST API ",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Daigo Tanaka, Anelen Co., LLC",
    url="https://github.com/anelendata/tap-rest-api",

    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: Apache Software License",

        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",

        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],

    install_requires=[
        "attrs>=18.1.0",
        "backoff==1.8.0",
        "getschema>=0.2.7",
        "jsonschema==2.6.0",
        "python-dateutil>=2.7.3",
        "requests>=2.20.0",
        "simplejson==3.11.1",
        "singer-python>=5.2.0",
    ],
    entry_points="""
    [console_scripts]
    tap-rest-api=tap_rest_api:main
    """,
    packages=["tap_rest_api"],
    package_data={
        # Use MANIFEST.ini
    },
    include_package_data=True
)
