#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap_rest_api",
    version="0.1.2",
    description="Singer.io tap for extracting data",
    author="Anelen Co., LLC",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
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
        # "schemas": ["tap_rest_api/schemas/*.json"]
    },
    include_package_data=False
)
