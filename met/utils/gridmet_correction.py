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

import os
from datetime import datetime
from numpy import hstack

from matplotlib import pyplot as plt

from met.mesonet import Mesonet
from met.thredds import GridMet


def correct_pr_mesonet_gridmet(start, end, lat, lon):
    s, e = datetime.strptime(start, '%Y-%m-%d'), \
           datetime.strptime(end, '%Y-%m-%d')
    gridmet = GridMet('pr', start=s, end=e,
                      lat=lat, lon=lon)
    gridmet_ppt = gridmet.get_point_timeseries()
    gridmet_ppt = gridmet_ppt.values

    mco = Mesonet(LOLO_MCO, start=start, end=end)
    mesonet_ppt = mco.mesonet_ppt(daily=True)
    mesonet_ppt = mesonet_ppt['Precipitation'].values
    mesonet_ppt = mesonet_ppt.reshape((mesonet_ppt.shape[0], 1))
    meso_gs_ppt, grid_gs_ppt = mesonet_ppt[121:273], gridmet_ppt[121:273]
    comp = hstack((meso_gs_ppt, grid_gs_ppt))
    pass


def correct_etr_mesonet_gridmet(start, end, lat, lon):
    s, e = datetime.strptime(start, '%Y-%m-%d'), \
           datetime.strptime(end, '%Y-%m-%d')
    gridmet = GridMet('etr', start=s, end=e,
                      lat=lat, lon=lon)
    gridmet_etr = gridmet.get_point_timeseries()
    gridmet_etr = gridmet_etr.values

    mco = Mesonet(LOLO_MCO, start=start, end=end)
    mesonet_daily = mco.mesonet_etr(lat=46.3, elevation=1000.0)
    mesonet_etr = mesonet_daily['ETR'].values

    plt.plot(gridmet_etr[121:273], label='gridmet')
    plt.plot(mesonet_etr[121:273], label='mesonet')
    plt.xlabel('GROWING SEASON DAY (MAY 01 - SEP 30)')
    plt.ylabel('Tall Crop Reference ET (mm) daily')
    plt.legend()
    plt.show()

    ma_ratio = mesonet_etr[121:273].sum() / gridmet_etr[121:273].sum()
    print('mesonet - gridmet ratio: {}'.format(ma_ratio))
    ma_ratio = gridmet_etr[121:273].sum() / mesonet_etr[121:273].sum()
    print('gridmet - mesonet ratio: {}'.format(ma_ratio))


if __name__ == '__main__':
    home = os.path.expanduser('~')
    for yr in ['2018']:
        START = '{}-01-01'.format(yr)
        END = '{}-12-31'.format(yr)
        LAT = 46.748
        LON = -114.13
        LOLO_MCO = os.path.join(home, 'IrrigationGIS', 'lolo',
                                'mesonet', 'Lolo Campground Weather MCO 2018.csv')
        correct_pr_mesonet_gridmet(START, END, LAT, LON)
        # correct_etr_mesonet_gridmet(START, END, LAT, LON)
# ========================= EOF ====================================================================
