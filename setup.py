#!/usr/bin/env python
from setuptools import setup

VERSION = "0.1.3a0"

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="tap_rest_api",
    version=VERSION,
    description="Singer.io tap for extracting data from any REST API ",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Daigo Tanaka, Anelen Co., LLC",
    url="https://github.com/anelendata/tap_rest_api",

    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: Apache Software License",

        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",

        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],

    py_modules=["tap_rest_api"],
    install_requires=[
        'attrs==18.1.0',
        'backoff==1.3.2',
        'python-dateutil>=2.7.3',
        'requests>=2.20.0',
        'singer-python==5.0.15',
    ],
    entry_points="""
    [console_scripts]
    tap_rest_api=tap_rest_api:main
    """,
    packages=["tap_rest_api"],
    package_data = {
        # Use MANIFEST.ini
    },
    include_package_data=True
)
