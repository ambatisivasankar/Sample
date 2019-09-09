#!/usr/bin/env python
from setuptools import setup, find_packages

requirements = ["dirigible==0.2.7", "py4jdbc==0.1.7.0"]

long_description = ''

exec(compile(open("squark/version.py").read(), "squark/version.py", 'exec'))
VERSION = __version__  # noqa

setup(name='squark',
      version=VERSION,
      packages=find_packages(),
      author='Thom Neale',
      author_email='tneale@massmutual.com',
      url='http://github.com/massmutual/squark',
      description='Experimental ingestion framework',
      long_description=long_description,
      platforms=['any'],
      entry_points='''
          [console_scripts]
          squark = squark.__main__:cli
      ''',
      install_requires=requirements,
      classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
      ]
)
