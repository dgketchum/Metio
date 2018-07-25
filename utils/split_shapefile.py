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
# limitations under the License.To
# ===============================================================================

import os

from fiona import open as fiopen
from fiona.crs import from_epsg


def split_shapefile_by_attribute(vector, attribute, out_loc, crs=4326):
    dct = {}
    with fiopen(vector, 'r', driver='ESRI Shapefile') as src:
        for feat in src:
            try:
                dct[feat['properties'][attribute]].append(feat)
            except KeyError:
                dct[feat['properties'][attribute]] = [feat]
        meta = src.schema

    for key, val in dct.items():
        new_shape = os.path.join(out_loc, 'MT_{}'.format(key))
        with fiopen(new_shape, 'w', driver='ESRI Shapefile',
                    schema=meta, crs=from_epsg(crs)) as dst:
            for feat in val:
                dst.write(feat)
        print(new_shape)


if __name__ == '__main__':
    home = os.path.expanduser('~')
    location = os.path.join(home, 'IrrigationGIS', 'Statewide_Irrigation_Shapefile')
    in_shape = os.path.join(location, 'Statewide_MTSPCS_32100.shp')
    out_dir = os.path.join(location, 'by_huc_8')
    split_shapefile_by_attribute(in_shape, 'Huc_8', out_dir, crs=32100)
    pass
# ========================= EOF ====================================================================
