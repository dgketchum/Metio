# ===============================================================================
# Copyright 2017 dgketchum
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===============================================================================

import os

from setuptools import setup

os.environ['TRAVIS_CI'] = 'True'
setup_kwargs = {}

try:
    from setuptools import setup
    # setup_kwargs = {'entry_points': {'console_scripts': ['metio=metio.metio_cli:cli_runner']}}
except ImportError:
    from distutils.core import setup
    # setup_kwargs = {'scripts': ['bin/metio/metio_cli']}

with open('README.txt') as f:
    readme = f.read()

tag = '0.0.21'

setup(name='metio',
      version=tag,
      description='Very simple API to download gridded meteorology data from the web',
      long_description=readme,
      setup_requires=['nose>=1.0'],
      py_modules=['met'],
      license='Apache',
      classifiers=[
          'Intended Audience :: Science/Research',
          'Topic :: Scientific/Engineering :: GIS',
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3.6'],
      keywords='landsat gridded meteorology hydrology remote sensing',
      author='David Ketchum',
      author_email='dgketchum@gmail.com',
      platforms='Posix; MacOS X; Windows',
      packages=['met'],
      download_url='https://github.com/{}/{}/archive/{}.tar.gz'.format('dgketchum', 'Metio', tag),
      url='https://github.com/dgketchum/metio',
      test_suite='tests.test_suite.suite',
      install_requires=['numpy', 'geopy', 'pandas', 'requests', 'fiona',
                        'future', 'xarray', 'pyproj', 'rasterio', 'xlrd',
                        'SatelliteImage', 'bs4', 'netcdf4'],
      **setup_kwargs)


# ============= EOF ==============================================================
