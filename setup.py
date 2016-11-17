#!/usr/bin/env python

from setuptools import setup

setup(
    name='fatman-clients',
    version='0.1.dev0',
    py_modules=['fdaemon'],
    license='GPL3',
    install_requires=[
        'click>=6.6',
        'click-log>=0.1.4',
        'six>=1.10.0',
        'requests>=2.9.1',
        ],
    entry_points='''
        [console_scripts]
        fdaemon=fdaemon:main
        ''',
    )
