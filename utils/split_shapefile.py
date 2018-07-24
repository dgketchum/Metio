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

from fiona import open as fiopen


def split_shapefile_by_attribute(vector, attribute):
    dct = {}
    with fiopen(vector, 'r', driver='ESRI Shapefile') as src:
        for feat in src:
            try:
                dct['Huc_8'].append(feat)
            except KeyError:
                dct['Huc_8'] = [feat]
            break
        print(dct)


if __name__ == '__main__':
    home = os.path.expanduser('~')
    shape = os.path.join(home, 'IrrigationGIS', 'Statewide_Irrigation_Shapefile',
                         'Statewide_Irrig_07_10_18.shp')
    split_shapefile_by_attribute(shape, 'huc 8')
    pass
# ========================= EOF ====================================================================
