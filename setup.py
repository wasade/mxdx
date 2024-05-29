"""mxdx: multiplexing and demultiplexing."""
import versioneer
from setuptools import setup, find_packages


setup(name='mxdx',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      license='BSD-3-Clause',
      author='Daniel McDonald',
      author_email='damcdonald@ucsd.edu',
      packages=find_packages(),
      install_requires=[
          'click',
          'polars',
      ],
      entry_points='''
          [console_scripts]
          mxdx=mxdx.cli:cli
      ''')
