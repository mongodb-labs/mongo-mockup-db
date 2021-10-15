#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.version_info[:2] < (2, 7):
    raise RuntimeError("Python version >= 2.7 required.")

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('CHANGELOG.rst') as changelog_file:
    changelog = changelog_file.read().replace('.. :changelog:', '')

setup(
    name='mockupdb',
    version='1.8.1',
    description="MongoDB Wire Protocol server library",
    long_description=readme + '\n\n' + changelog,
    author="A. Jesse Jiryu Davis",
    author_email='jesse@mongodb.com',
    url='https://github.com/ajdavis/mongo-mockup-db',
    packages=['mockupdb'],
    package_dir={'mockupdb': 'mockupdb'},
    include_package_data=True,
    install_requires=['pymongo>=3'],
    license="Apache License, Version 2.0",
    zip_safe=False,
    keywords=["mongo", "mongodb", "wire protocol", "mockupdb", "mock"],
    python_requires=">=2.7,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*",
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        "License :: OSI Approved :: Apache Software License",
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    test_suite='tests',
    tests_require=[]
)
