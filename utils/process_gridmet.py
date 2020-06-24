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

import os
from calendar import monthrange
from datetime import datetime

from numpy import mean
from fiona import open as fiona_open
from rasterio import open as raster_open
from rasterio.mask import mask
from shapely.geometry import Polygon

from met.thredds import GridMet
from bounds import RasterBounds, GeoBounds
from sat_image.image import Landsat8


class CatchmetGridmet():

    def __init__(self, year, month, _id, variable='pr'):
        home = os.path.expanduser('~')
        self.netcdf = os.path.join(home, 'IrrigationGIS', 'lolo', 'gridmet_ncdf')
        self.tif = os.path.join(home, 'IrrigationGIS', 'lolo', 'tif')
        self.landsat = os.path.join(home, 'IrrigationGIS', 'lolo', 'LC80410282016158LGN01')
        self.catchments = os.path.join(home, 'IrrigationGIS', 'lolo', 'shp', 'Lolo_WB_Model_Calibration_Catchments_32611.shp')
        self.project_crs = {'init': 'epsg:32611'}

        self.year = year
        self.month = month
        self.id = _id
        self.variable = variable
        self.tif_name = os.path.join(self.tif, 'F{}_{}_{}_{}.tif'.format(_id, variable, year, month))
        self.catchment = self._get_feature()

        self.arr = None
        self.geometry = None
        self.gridmet = None

    def _get_feature(self):
        with fiona_open(self.catchments, 'r') as src:
            assert src.crs == self.project_crs
            for f in src:
                if f['properties']['Id'] == self.id:
                    catchment = Polygon(f['geometry']['coordinates'][0])
                    return catchment
            raise NotImplementedError()

    def _get_landsat_image(self):
        l8 = Landsat8(self.landsat)
        bounds = RasterBounds(affine_transform=l8.rasterio_geometry['transform'], profile=l8.rasterio_geometry)
        proj_bounds = bounds.to_epsg(32611)
        clip = proj_bounds.get_shapely_polygon()
        return l8.rasterio_geometry, bounds, clip

    def _clip_to_catchment(self):

        self.gridmet.save_raster(self.arr, self.geometry, self.tif_name)

        with raster_open(self.tif_name) as src:
            out_arr, out_trans = mask(src, [self.catchment], crop=True, all_touched=True)
            out_meta = src.meta.copy()
            out_meta.update({'driver': 'GTiff',
                             'height': out_arr.shape[1],
                             'width': out_arr.shape[2],
                             'transform': out_trans})

        with raster_open(self.tif_name, 'w', **out_meta) as dst:
            dst.write(out_arr)

        return out_arr

    def get_monthly_gridmet(self):

        self.geometry, bounds, clip = self._get_landsat_image()
        assert self.geometry['crs'].data == self.project_crs

        days = monthrange(self.year, self.month)[1]
        s, e = datetime(self.year, self.month, 1), datetime(self.year, self.month, days)

        kwargs = dict(start=s, end=e, bbox=bounds, clip_feature=clip, target_profile=self.geometry)

        if self.variable == 'pr':
            self.gridmet = GridMet(self.variable, **kwargs)
            path = os.path.join(self.netcdf, '{}_{}.nc'.format(self.variable, self.year))
            arr = self.gridmet.get_area_timeseries(file_url=path)
            self.arr = arr.sum(axis=0)

        elif self.variable == 'temp':
            path = os.path.join(self.netcdf, 'tmmx_{}.nc'.format(self.year))
            self.gridmet = GridMet('tmmx', **kwargs)
            max = self.gridmet.get_area_timeseries(file_url=path)

            path = os.path.join(self.netcdf, 'tmmn_{}.nc'.format(self.year))
            self.gridmet = GridMet('tmmn', **kwargs)
            min = self.gridmet.get_area_timeseries(file_url=path)

            self.arr = mean([min, max], axis=0)

        elif self.variable == 'elev':
            self.gridmet = GridMet(self.variable, **kwargs)
            path = os.path.join(self.netcdf, 'metdata_elevationdata.nc'.format(self.variable))
            self.arr = self.gridmet.get_data_subset(file_url=path)

        else:
            raise NotImplementedError()

        arr = self._clip_to_catchment()

        return arr


if __name__ == '__main__':
    gridmet = CatchmetGridmet(2014, 1, _id=8, variable='elev')
    array = gridmet.get_monthly_gridmet()
    pass
# ========================= EOF ====================================================================
