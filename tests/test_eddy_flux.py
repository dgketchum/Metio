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

import unittest

import os
import json
from datetime import datetime
from fiona import open as fopen

from met.eddy_flux import FluxSite


class EddyTowerTestCase(unittest.TestCase):
    def setUp(self):
        self.data_save_loc = 'tests/data/points/flux_locations_lathuille.json'
        self.data_perm = 'met/data/points/flux_locations_lathuille.json'
        self.select_sites = ['BR']
        self.shape_out = 'met/data/flux_locations_lathuille.shp'
        self.site = 'US-FPe'
        self.fmt = '%Y-%m-%d'
        self.fpe_start, self.fpe_end = '2000-01-01', '2006-12-31'

    def test_load_site_data(self):
        site = self.site
        flux = FluxSite(site_key=site, json_file=self.data_perm)
        data = flux.load_site_data()
        start = datetime.strptime(self.fpe_start, self.fmt)
        end = datetime.strptime(self.fpe_end, self.fmt)
        delta = end - start
        days = delta.days
        self.assertEqual(data.shape, (days + 1, 122))

    def test_network_json_few_sites(self):
        flux = FluxSite(json_file=self.data_save_loc,
                        country_abvs=self.select_sites)
        data_dict = flux.data
        self.assertIsInstance(data_dict, dict)
        br = data_dict['BR-Ban']
        self.assertEqual(br['Latitude'], -9.8244)
        self.assertEqual(br['Longitude'], -50.1591)
        self.assertEqual(len(br['csv_url']), 4)
        with open(self.data_save_loc) as f:
            d = json.load(f)
            self.assertIsInstance(d, dict)
        os.remove(self.data_save_loc)

    def test_network_json_all_sites(self):
        flux = FluxSite(json_file=self.data_save_loc)
        data_dict = flux.data
        self.assertEqual(len(data_dict.keys()), 252)
        with open(self.data_save_loc) as f:
            d = json.load(f)
            self.assertIsInstance(d, dict)
        os.remove(self.data_save_loc)

    def test_local_json_loader(self):
        flux = FluxSite(json_file=self.data_perm)
        data = flux.data
        self.assertEqual(len(data.keys()), 252)
        self.assertIsInstance(data, dict)

    def test_write_shapefile(self):
        flux = FluxSite(self.data_perm)
        data = flux.data
        flux.write_locations_to_shp(data, self.shape_out)
        with fopen(self.shape_out, 'r') as shp:
            count = 0
            for feature in shp:
                count += 1
            self.assertEqual(count, 252)


if __name__ == '__main__':
    unittest.main()

# ===============================================================================
