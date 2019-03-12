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
import requests
import pandas as pd


# script for returning elevation from lat, long, based on open elevation data
# which in turn is based on SRTM
def get_elevation(lat, long):
    query = 'https://nationalmap.gov/epqs/pqs.php?units=feet' \
            '&output=json&x={}&y={}'.format(long, lat)
    r = requests.get(query).json()
    elevation = r['USGS_Elevation_Point_Query_Service']['Elevation_Query']['Elevation'] * 0.3048
    return elevation


if __name__ == '__main__':
    pass
# ========================= EOF ====================================================================
