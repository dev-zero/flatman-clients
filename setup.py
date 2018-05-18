#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='fatman-clients',
    version='0.1.dev0',
    packages=find_packages(),
    license='GPL3',
    install_requires=[
        'click>=6.6',
        'click-log>=0.1.4',
        'six>=1.10.0',
        'requests>=2.9.1',
        'terminaltables>=3.1.0',
        'dpath>=1.4.0',
        'periodictable>=1.5.0',
        ],
    entry_points='''
        [console_scripts]
        fdaemon=fatman_clients.fdaemon:main
        fclient=fatman_clients.fclient:cli
        ''',
    )
