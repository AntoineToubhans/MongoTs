# !/usr/bin/env python
from setuptools import setup, find_packages

requirements = [
    'pandas',
    'parse',
    'pymongo',
]

setup_requirements = [
    'bumpversion',
]

test_requirements = [
    'flake8',
    'mongomock',
    'mypy'
    'pytest',
    'unittest-data-provider',
]

description = 'A fast API for storing and querying time series in MongoDb'

setup(
    name='mongots',
    packages=find_packages(include=['mongots']),
    version='0.2.0',
    description=description,
    long_description=description,
    author='Antoine Toubhans',
    license='MIT',
    author_email='antoine@toubhans.org',
    url='https://github.com/AntoineToubhans/MongoTs',
    keywords=['mongo', 'pymongo', 'timeserie', ],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    install_requires=requirements,
    tests_require=test_requirements,
    setup_requires=setup_requirements,
)
