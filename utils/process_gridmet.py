# ===============================================================================
# Copyright 2018 dgketchum
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
from __future__ import print_function, absolute_import

from future.standard_library import hooks

import os
from calendar import monthrange
from datetime import datetime

from numpy import mean
from fiona import open as fiona_open

from met.thredds import GridMet
from bounds import GeoBounds

home = os.path.expanduser('~')
LOLO_BOUNDS = GeoBounds(west=-114.8, east=-113.85,
                        south=46.5, north=46.9)

NETCDF = os.path.join(home, 'IrrigationGIS', 'lolo', 'gridmet_ncdf')
TIF = os.path.join(home, 'IrrigationGIS', 'lolo', 'tif')
CATCHMENTS = os.path.join(home, 'IrrigationGIS', 'lolo', 'shp', 'Lolo_WB_Model_Calibration_Catchments_wgs.shp')
YEARS = [x for x in range(2014, 2020)]


def get_feature(_id):
    with fiona_open(CATCHMENTS, 'r') as src:
        for f in src:
            if f['properties']['Id'] == _id:
                catchment = f['geometry']
                return catchment
        raise NotImplementedError()


def get_monthly_gridmet(year, month, _id, variable='pr'):

    catchment = get_feature(_id)
    days = monthrange(year, month)[1]
    s, e = datetime(year, month, 1), datetime(year, month, days)
    gridmet = GridMet(variable, start=s, end=e, bbox=LOLO_BOUNDS, clip_feature=catchment)

    if variable == 'pr':
        path = os.path.join(NETCDF, '{}_{}.nc'.format(variable, year))
        arr = gridmet.get_area_timeseries(file_url=path)
        pr_sum = arr.sum(axis=0)

    elif variable == 'temp':
        path = os.path.join(NETCDF, 'tmmx_{}.nc'.format(year))
        max = gridmet.get_area_timeseries(file_url=path)
        path = os.path.join(NETCDF, 'tmmn_{}.nc'.format(year))
        min = gridmet.get_area_timeseries(file_url=path)
        daily_avg_temp = mean((min, max), keepdims=True)

    else:
        raise NotImplementedError()


if __name__ == '__main__':
    get_monthly_gridmet(2014, 1, _id=1, variable='temp')
# ========================= EOF ====================================================================
