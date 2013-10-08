# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

classifiers = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: Apache Software License",
    "Programming Language :: Python",
    "Programming Language :: Python :: 2.7",
    "Programming Language :: Python :: 3.3",
    "Topic :: Database",
    "Topic :: Software Development",
    "Topic :: Software Development :: Testing",
]


setup(
     name='test.cassandra',
     version='0.1.0',
     description='automatically setups a cassandra instance in a temporary directory, and destroys it after testing',
     long_description='',
     classifiers=classifiers,
     keywords=[],
     author='Takeshi Komiya',
     author_email='i.tkomiya at gmail.com',
     url='http://bitbucket.org/tk0miya/test.cassandra',
     license='Apache License 2.0',
     packages=find_packages('src'),
     package_dir={'': 'src'},
     package_data = {'': ['buildout.cfg']},
     include_package_data=True,
     install_requires=[
         'pycassa',
     ],
     extras_require=dict(
         test=[
             'Nose',
             'pep8',
         ],
     ),
     test_suite='nose.collector',
)
