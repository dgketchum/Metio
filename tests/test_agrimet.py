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
import unittest
import json
import requests
from fiona import open as fopen
from numpy import isnan

from met.agrimet import Agrimet
from sat_image.image import Landsat8


class TestAgrimet(unittest.TestCase):
    def setUp(self):
        self.root = os.path.dirname(__file__)
        self.point_file = os.path.join(self.root, 'data', 'points', 'agrimet_location_test.shp')
        self.station_info = 'https://www.usbr.gov/pn/agrimet/agrimetmap/usbr_map.json'
        self.dirname_image = os.path.join(self.root, 'data', 'image_test', 'lc8_image')
        self.site_ids = ['umhm', 'robi', 'hntu', 'faln', 'mdxo', 'mdso', 'masw']
        self.fetch_site = 'drlm'
        self.gp_site = 'bozm'
        self.pn_site = 'abei'
        self.outside_PnGp_sites = ['pvan', 'mdki', 'laju']
        self.points_dir = os.path.join(self.root, 'data', 'points')
        self.out_shape = os.path.join(self.root, 'data', 'points', 'agmet_station_write_test.shp')
        self.test_locations = os.path.join(self.root, 'data', 'points', 'agmet_station_off_location.shp')
        self.start, self.end = '2015-05-01', '2015-05-05'
        self.excepted_sites = ['clon', 'clvn', 'csdu', 'hntu', 'pngo', 'defo', 'echo', 'hrmo',
                               'lggu', 'evfu', 'libw', 'psfi', 'mdno', 'mdxo', 'mdeo', 'mwso',
                               'olth02', 'olth01', 'paro', 'defo', 'sugi', 'rxgi', 'shli', 'ifai']
        self.start_season, self.end_season = '2015-04-15', '2015-10-15'
        self.all_stations = ['abei',
                             'acki',
                             'afty',
                             'agko',
                             'ahti',
                             'anvn',
                             'arao',
                             'bano',
                             'bato',
                             'bewo',
                             'bfam',
                             'bfgi',
                             'bftm',
                             'bkvo',
                             'blbu',
                             'blcu',
                             'blou',
                             'bndw',
                             'boii',
                             'bomt',
                             'bozm',
                             'brgm',
                             'brju',
                             'brko',
                             'brtm',
                             'bucu',
                             'bvpc',
                             'cdai',
                             'cedc',
                             'cedu',
                             'chaw',
                             'chvo',
                             'cjdw',
                             'ckvy',
                             'clon',
                             'clvn',
                             'covm',
                             'crnu',
                             'crsm',
                             'crvo',
                             'csdu',
                             'csvu',
                             'cvan',
                             'defo',
                             'deni',
                             'dlnm',
                             'dlt01',
                             'drfu',
                             'drlm',
                             'drpw',
                             'dtro',
                             'ducu',
                             'dwni',
                             'ebri',
                             'echo',
                             'efhw',
                             'elmu',
                             'eurn',
                             'evfu',
                             'evty',
                             'fafi',
                             'faln',
                             'flou',
                             'fogo',
                             'frnu',
                             'frt02',
                             'fthi',
                             'gcdw',
                             'gdvi',
                             'gerw',
                             'gfmt',
                             'gfri',
                             'glgm',
                             'golw',
                             'grei',
                             'grtu',
                             'gun01',
                             'hami',
                             'hcko',
                             'hdru',
                             'hero',
                             'hntu',
                             'hoxo',
                             'hrfo',
                             'hrhw',
                             'hrlm',
                             'hrmo',
                             'huan',
                             'hvmt',
                             'ichi',
                             'ifai',
                             'igri',
                             'imbo',
                             'jvwm',
                             'kflo',
                             'kflw',
                             'ktbi',
                             'lako',
                             'laku',
                             'lbrw',
                             'legw',
                             'lewu',
                             'lggu',
                             'lgiu',
                             'libw',
                             'lidw',
                             'lkpo',
                             'lmmm',
                             'lndn',
                             'loau',
                             'lofi',
                             'loro',
                             'mali',
                             'masw',
                             'matm',
                             'mdeo',
                             'mdfo',
                             'mdki',
                             'mdno',
                             'mdso',
                             'mdto',
                             'mdxo',
                             'mnpi',
                             'mnru',
                             'mnti',
                             'moan',
                             'mrso',
                             'msvn',
                             'muru',
                             'mwsm',
                             'mwso',
                             'nepu',
                             'nmpi',
                             'nsvn',
                             'odsw',
                             'olth01',
                             'olth02',
                             'omaw',
                             'onto',
                             'osgi',
                             'owei',
                             'panu',
                             'paro',
                             'paru',
                             'pcyo',
                             'pelu',
                             'pici',
                             'plvu',
                             'pmai',
                             'pngo',
                             'pobo',
                             'psfi',
                             'psti',
                             'pvan',
                             'pwln',
                             'rbym',
                             'rdbm',
                             'rdhu',
                             'rgbi',
                             'robi',
                             'rogn',
                             'roso',
                             'rpti',
                             'rrci',
                             'rrii',
                             'rthi',
                             'rxgi',
                             'sacw',
                             'sbmw',
                             'scpu',
                             'shli',
                             'sigm',
                             'silw',
                             'slwi',
                             'smvn',
                             'snkn',
                             'snsu',
                             'snwu',
                             'span',
                             'spfu',
                             'spli',
                             'spvu',
                             'ssvn',
                             'stvn',
                             'sugi',
                             'sutu',
                             'svwm',
                             'swmn',
                             'tabi',
                             'teri',
                             'tfgi',
                             'tlkc',
                             'tosm',
                             'trfm',
                             'trmu',
                             'trpu',
                             'trti',
                             'twfi',
                             'umhm',
                             'vecu',
                             'vrnu',
                             'wrdo',
                             'wssm']

    def test_instantiate_Agrimet(self):
        """ Test object instantiation.
        :return: 
        """
        ag = Agrimet(start_date='2000-01-01', end_date='2000-12-31',
                     station=self.fetch_site,
                     sat_image=Landsat8(self.dirname_image))
        self.assertIsInstance(ag, Agrimet)

    def test_load_station_data(self):
        """ Test load all station data from web.
        :return: 
        """
        r = requests.get(self.station_info)
        stations = json.loads(r.text)
        self.assertIsInstance(stations, dict)

    def test_find_closest_station(self):
        """ Test find closest agrimet station to any point.
        :return: 
        """
        coords = []
        with fopen(self.point_file, 'r') as src:
            for feature in src:
                coords.append(feature['geometry']['coordinates'])

        for coord in coords:
            agrimet = Agrimet(lon=coord[0], lat=coord[1])
            self.assertTrue(agrimet.station in self.site_ids)

    def test_find_image_station(self):
        """ Test find closest agrimet station to Landsat image centroid.
        :return: 
        """
        l8 = Landsat8(self.dirname_image)
        agrimet = Agrimet(sat_image=l8)
        self.assertEqual(agrimet.station, self.fetch_site)

    def test_fetch_data(self):
        """ Test download agrimet data within time slice.
        Test refomatting of data, test unit converstion to std units.
        Checked data is in a Pandas.DataFrame object.
        :return: 
        """
        agrimet = Agrimet(station=self.fetch_site, start_date='2015-01-01',
                          end_date='2015-12-31', interval='daily')

        raw = agrimet.fetch_met_data(return_raw=True)
        formed = agrimet.fetch_met_data()

        a = raw.iloc[1, :].tolist()
        b = formed.iloc[1, :].tolist()

        # dates equality
        self.assertEqual(a[0], b[0])
        self.assertEqual(a[0], '2015-01-02')
        # in to mm
        self.assertEqual(a[2], b[2] / 25.4)
        # deg F to deg C
        self.assertAlmostEqual(a[4], b[4] * 1.8 + 32, delta=0.01)
        # in to mm
        self.assertEqual(a[7], b[7] / 25.4)
        # Langleys to J m-2
        self.assertEqual(a[9], b[9] / 41868.)
        # mph to m sec-1
        self.assertEqual(a[12], b[12] / 0.44704)

    def test_find_sites(self):
        with fopen(self.test_locations, 'r') as src:
            for feat in src:
                lat, lon = feat['geometry']['coordinates'][1], feat['geometry']['coordinates'][0]
                expected_site = feat['properties']['siteid']
                #  don't test on neighboring sites
                if expected_site not in self.excepted_sites:
                    agrimet = Agrimet(lat=lat, lon=lon, start_date=self.start, end_date=self.end)
                    found_site = agrimet.station
                    self.assertEqual(expected_site, found_site)

    def test_fetch_data_many_stations(self):
        """ Test download nultiple agrimet station data download.
        This runs through a list of stations, reformats data, checks unit conversion,
        and Pandas.DataFrame
        :return:
        """
        for site in self.outside_PnGp_sites:
            agrimet = Agrimet(station=site, start_date='2015-05-15',
                              end_date='2015-05-15', interval='daily')
            raw = agrimet.fetch_met_data(return_raw=True)
            formed = agrimet.fetch_met_data()
            params = ['et', 'mm', 'pc', 'sr', 'wr']
            for param in params:
                key = '{}_{}'.format(site, param)
                converted = formed[param.upper()].values.flatten()[0]
                unconverted = raw[key].values.flatten()[0]
                if param in ['et', 'pc']:
                    unconverted *= 25.4
                if param == 'mm':
                    unconverted = (unconverted - 32) * 5 / 9
                if param == 'sr':
                    unconverted *= 41868.
                if param == 'wr':
                    unconverted *= 1609.34
                if isnan(converted):
                    pass
                elif agrimet.empty_df:
                    pass
                else:
                    self.assertAlmostEqual(converted, unconverted, delta=0.01)

    def test_great_plains_met(self):

        a = Agrimet(station=self.gp_site, start_date=self.start,
                    end_date=self.end, interval='daily')
        df = a.fetch_met_data()

        self.assertIsInstance(a, Agrimet)

    def test_pacific_nw_met(self):

        a = Agrimet(station=self.pn_site, start_date=self.start,
                    end_date=self.end, interval='daily')
        df = a.fetch_met_data()

        self.assertIsInstance(a, Agrimet)

    def test_great_plains_crop(self):

        a = Agrimet(station=self.gp_site, start_date=self.start, end_date=self.end, interval='daily')
        df = a.fetch_crop_data()

        self.assertIsInstance(a, Agrimet)

    def test_pacific_nw_crop(self):

        a = Agrimet(station=self.pn_site, start_date=self.start, end_date=self.end, interval='daily')
        df = a.fetch_crop_data()

        self.assertIsInstance(a, Agrimet)

    def test_web_retrieval_all_stations_met(self):

        for s in self.all_stations:
            a = Agrimet(station=s, start_date=self.start_season,
                        end_date=self.end_season, interval='daily')
            try:
                a.fetch_met_data()
                print('{} appears valid'.format(a.station))
            except:
                print('{} appears invalid'.format(a.station))

    def test_web_retrieval_all_stations_crop(self):
        fails = []
        for s in self.all_stations:
            a = Agrimet(station=s, start_date=self.start_season,
                        end_date=self.end_season, interval='daily')
            try:
                a.fetch_crop_data()
                print('{} appears valid'.format(a.station))
            except Exception as e:
                print('{} appears invalid: {}'.format(a.station, e))
                fails.append(a.station)
        print(fails)

    def test_write_agrimet_shapefile(self):

        agrimet = Agrimet(write_stations=True)
        station_data = agrimet.load_stations()
        epsg = '4326'
        outfile = self.out_shape
        agrimet.write_agrimet_sation_shp(station_data, epsg, outfile)
        with fopen(outfile, 'r') as shp:
            count = 0
            for _ in shp:
                count += 1
        self.assertEqual(186, count)
        file_list = os.listdir(self.points_dir)
        for f in file_list:
            if 'write_test' in f:
                os.remove(os.path.join(self.points_dir, f))


if __name__ == '__main__':
    unittest.main()

# ===============================================================================
