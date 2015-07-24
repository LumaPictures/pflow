#!/usr/bin/env python
from setuptools import setup

import pflow

setup(
    name='pflow',
    version=pflow.__version__,

    author='Chris Lyon',
    author_email='flushot@gmail.com',

    description='Flow Based Programming for Python',
    long_description=open('README.md').read(),

    url='https://github.com/Flushot/pflow',

    license='Apache License 2.0',
    classifiers=[
        'Intended Audience :: Developers'
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: Apache Software License',
        ],

    install_requires=[
        'haigha',
        'argparse',
        'enum34'
    ],

    test_suite='pflow',
    tests_require=[
        'mock'
    ]
)
