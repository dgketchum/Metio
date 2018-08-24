# =============================================================================================
# Copyright 2017 dgketchum
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# =============================================================================================
from __future__ import print_function, absolute_import

import io
import json
import requests
from requests.compat import urlencode, OrderedDict
from datetime import datetime
from fiona import collection
from fiona.crs import from_epsg
from geopy.distance import geodesic
from pandas import read_table, to_datetime, date_range, read_csv, to_numeric

STATION_INFO_URL = 'https://www.usbr.gov/pn/agrimet/agrimetmap/usbr_map.json'
AGRIMET_MET_REQ_SCRIPT_PN = 'https://www.usbr.gov/pn-bin/agrimet.pl'
AGRIMET_CROP_REQ_SCRIPT_PN = 'https://www.usbr.gov/pn/agrimet/chart/{}{}et.txt'
AGRIMET_MET_REQ_SCRIPT_GP = 'https://www.usbr.gov/gp-bin/agrimet_archives.pl'
AGRIMET_CROP_REQ_SCRIPT_GP = 'https://www.usbr.gov/gp-bin/et_summaries.pl?station={}&year={}&submit2=++Submit++'
# in km
EARTH_RADIUS = 6371.

WEATHER_PARAMETRS_UNCONVERTED = [('DATETIME', 'Date - [YYYY-MM-DD]'),
                                 ('ET', 'Evapotranspiration Kimberly-Penman - [in]'),
                                 ('ETos', 'Evapotranspiration ASCE-EWRI Grass - [in]'),
                                 ('ETrs', 'Evapotranspiration ASCE-EWRI Alfalfa - [in]'),
                                 ('MM', 'Mean Daily Air Temperature - [F]'),
                                 ('MN', 'Minimum Daily Air Temperature - [F]'),
                                 ('MX', 'Maximum Daily Air Temperature - [F]'),
                                 ('PC', 'Accumulated Precipitation Since Recharge/Reset - [in]'),
                                 ('PP', 'Daily (24 hour) Precipitation - [in]'),
                                 ('PU', 'Accumulated Water Year Precipitation - [in]'),
                                 ('SR', 'Daily Global Solar Radiation - [langleys]'),
                                 ('TA', 'Mean Daily Humidity - [%]'),
                                 ('TG', 'Growing Degree Days - [base 50F]'),
                                 ('YM', 'Mean Daily Dewpoint Temperature - [F]'),
                                 ('UA', 'Daily Average Wind Speed - [mph]'),
                                 ('UD', 'Daily Average Wind Direction - [deg az]'),
                                 ('WG', 'Daily Peak Wind Gust - [mph]'),
                                 ('WR', 'Daily Wind Run - [miles]'),
                                 ]

WEATHER_PARAMETRS = [('DATETIME', 'Date', '[YYYY-MM-DD]'),
                     ('ET', 'Evapotranspiration Kimberly-Penman', '[mm]'),
                     ('ETos', 'Evapotranspiration ASCE-EWRI Grass', '[mm]'),
                     ('ETrs', 'Evapotranspiration ASCE-EWRI Alfalfa', '[mm]'),
                     ('MM', 'Mean Daily Air Temperature', '[C]'),
                     ('MN', 'Minimum Daily Air Temperature', '[C]'),
                     ('MX', 'Maximum Daily Air Temperature', '[C]'),
                     ('PC', 'Accumulated Precipitation Since Recharge/Reset', '[mm]'),
                     ('PP', 'Daily (24 hour) Precipitation', '[mm]'),
                     ('PU', 'Accumulated Water Year Precipitation', '[mm]'),
                     ('SR', 'Daily Global Solar Radiation', '[MJ m-2]'),
                     ('TA', 'Mean Daily Humidity', '[%]'),
                     ('TG', 'Growing Degree Days', '[base 50F]'),
                     ('YM', 'Mean Daily Dewpoint Temperature', '[C]'),
                     ('UA', 'Daily Average Wind Speed', '[m sec-1]'),
                     ('UD', 'Daily Average Wind Direction - [deg az]', '[deg az]'),
                     ('WG', 'Daily Peak Wind Gust', '[m sec-1]'),
                     ('WR', 'Daily Wind Run', '[m]')]

TARGET_COLUMNS = ['{a}_et', '{a}_etos', '{a}_etrs', '{a}_mm', '{a}_mn',
                  '{a}_mx', '{a}_pp', '{a}_pu', '{a}_sr', '{a}_ta', '{a}_tg',
                  '{a}_ua', '{a}_ud', '{a}_wg', '{a}_wr', '{a}_ym']

STANDARD_PARAMS = ['et', 'etos', 'etrs', 'mm', 'mn',
                   'mx', 'pp', 'pu', 'sr', 'ta', 'tg',
                   'ua', 'ud', 'wg', 'wr', 'ym']

ALL_STATIONS = {'abei': 'pn',
                'acki': 'pn',
                'afty': 'pn',
                'agko': 'pn',
                'ahti': 'pn',
                'anvn': 'pn',
                'arao': 'pn',
                'bano': 'pn',
                'bato': 'pn',
                'bewo': 'pn',
                'bfgi': 'pn',
                'bkvo': 'pn',
                'blbu': 'pn',
                'blcu': 'pn',
                'blou': 'pn',
                'bndw': 'pn',
                'boii': 'pn',
                'brju': 'pn',
                'brko': 'pn',
                'bucu': 'pn',
                'bvpc': 'pn',
                'cdai': 'pn',
                'cedc': 'pn',
                'cedu': 'pn',
                'chaw': 'pn',
                'chvo': 'pn',
                'cjdw': 'pn',
                'ckvy': 'pn',
                'covm': 'pn',
                'crnu': 'pn',
                'crsm': 'pn',
                'crvo': 'pn',
                'csdu': 'pn',
                'csvu': 'pn',
                'cvan': 'pn',
                'defo': 'pn',
                'deni': 'pn',
                'drfu': 'pn',
                'drlm': 'pn',
                'drpw': 'pn',
                'dtro': 'pn',
                'ducu': 'pn',
                'dwni': 'pn',
                'ebri': 'pn',
                'echo': 'pn',
                'efhw': 'pn',
                'elmu': 'pn',
                'eurn': 'pn',
                'evfu': 'pn',
                'evty': 'pn',
                'fafi': 'pn',
                'faln': 'pn',
                'flou': 'pn',
                'fogo': 'pn',
                'frnu': 'pn',
                'fthi': 'pn',
                'gcdw': 'pn',
                'gdvi': 'pn',
                'gerw': 'pn',
                'gfri': 'pn',
                'golw': 'pn',
                'grei': 'pn',
                'grtu': 'pn',
                'hami': 'pn',
                'hdru': 'pn',
                'hero': 'pn',
                'hntu': 'pn',
                'hoxo': 'pn',
                'hrfo': 'pn',
                'hrhw': 'pn',
                'hrmo': 'pn',
                'huan': 'pn',
                'ichi': 'pn',
                'ifai': 'pn',
                'igri': 'pn',
                'imbo': 'pn',
                'kflo': 'pn',
                'kflw': 'pn',
                'lako': 'pn',
                'laku': 'pn',
                'lbrw': 'pn',
                'legw': 'pn',
                'lewu': 'pn',
                'lggu': 'pn',
                'libw': 'pn',
                'lidw': 'pn',
                'lndn': 'pn',
                'loau': 'pn',
                'lofi': 'pn',
                'loro': 'pn',
                'mali': 'pn',
                'masw': 'pn',
                'mdfo': 'pn',
                'mdki': 'pn',
                'mdxo': 'pn',
                'mnpi': 'pn',
                'mnru': 'pn',
                'moan': 'pn',
                'mrso': 'pn',
                'msvn': 'pn',
                'muru': 'pn',
                'mwso': 'pn',
                'nepu': 'pn',
                'nmpi': 'pn',
                'nsvn': 'pn',
                'odsw': 'pn',
                'omaw': 'pn',
                'onto': 'pn',
                'osgi': 'pn',
                'owei': 'pn',
                'panu': 'pn',
                'paro': 'pn',
                'paru': 'pn',
                'pcyo': 'pn',
                'pelu': 'pn',
                'pici': 'pn',
                'plvu': 'pn',
                'pmai': 'pn',
                'pngo': 'pn',
                'pobo': 'pn',
                'psfi': 'pn',
                'psti': 'pn',
                'pvan': 'pn',
                'pwln': 'pn',
                'rdbm': 'pn',
                'rdhu': 'pn',
                'rgbi': 'pn',
                'robi': 'pn',
                'rogn': 'pn',
                'roso': 'pn',
                'rpti': 'pn',
                'rrci': 'pn',
                'rrii': 'pn',
                'rthi': 'pn',
                'rxgi': 'pn',
                'sacw': 'pn',
                'sbmw': 'pn',
                'scpu': 'pn',
                'shli': 'pn',
                'sigm': 'pn',
                'silw': 'pn',
                'slwi': 'pn',
                'smvn': 'pn',
                'snkn': 'pn',
                'snsu': 'pn',
                'snwu': 'pn',
                'span': 'pn',
                'spfu': 'pn',
                'spli': 'pn',
                'spvu': 'pn',
                'ssvn': 'pn',
                'stvn': 'pn',
                'sugi': 'pn',
                'sutu': 'pn',
                'swmn': 'pn',
                'tabi': 'pn',
                'teri': 'pn',
                'tfgi': 'pn',
                'tlkc': 'pn',
                'trmu': 'pn',
                'trpu': 'pn',
                'trti': 'pn',
                'twfi': 'pn',
                'vecu': 'pn',
                'vrnu': 'pn',
                'wrdo': 'pn',
                'gun01': 'pn',
                'dlt01': 'pn',
                'frt02': 'pn',
                'gfmt': 'gp',
                'rbym': 'gp',
                'bfam': 'gp',
                'bftm': 'gp',
                'bomt': 'gp',
                'bozm': 'gp',
                'brgm': 'gp',
                'brtm': 'gp',
                'clon': 'gp',
                'clvn': 'gp',
                'dlnm': 'gp',
                'glgm': 'gp',
                'hcko': 'gp',
                'hrlm': 'gp',
                'hvmt': 'gp',
                'jvwm': 'gp',
                'ktbi': 'gp',
                'lgiu': 'gp',
                'lkpo': 'gp',
                'lmmm': 'gp',
                'matm': 'gp',
                'mdeo': 'gp',
                'mdno': 'gp',
                'mdso': 'gp',
                'mdto': 'gp',
                'mnti': 'gp',
                'mwsm': 'gp',
                'svwm': 'gp',
                'tosm': 'gp',
                'trfm': 'gp',
                'umhm': 'gp',
                'wssm': 'gp',
                'olth01': 'gp',
                'olth02': 'gp'}


class Agrimet(object):
    def __init__(self, start_date=None, end_date=None, station=None,
                 interval=None, lat=None, lon=None, sat_image=None,
                 write_stations=False):

        self.station_info_url = STATION_INFO_URL
        self.station = station
        self.distance_from_station = None
        self.station_coords = None
        self.distances = None

        self.empty_df = True

        if not station and not write_stations:
            if not lat and not sat_image:
                raise ValueError('Must initialize agrimet with a station, '
                                 'an Image, or some coordinates.')
            if not sat_image:
                self.station = self.find_closest_station(lat, lon)
            else:

                lat = (sat_image.corner_ll_lat_product + sat_image.corner_ul_lat_product) / 2
                lon = (sat_image.corner_ll_lon_product + sat_image.corner_lr_lon_product) / 2
                self.station = self.find_closest_station(lat, lon)

        if station:
            self.find_station_coords()

        self.interval = interval

        if start_date and end_date:
            self.start = datetime.strptime(start_date, '%Y-%m-%d')
            self.end = datetime.strptime(end_date, '%Y-%m-%d')
            self.today = datetime.now()
            self.start_index = (self.today - self.start).days - 1
        try:
            self.region = ALL_STATIONS[self.station]
        except KeyError:
            pass

        self.rank = 0

    @property
    def params(self):
        return urlencode(OrderedDict([
            ('cbtt', self.station),
            ('interval', self.interval),
            ('format', 1),
            ('back', self.start_index)
        ]))

    def find_station_coords(self):
        station_data = self.load_stations()
        for feat in station_data['features']:
            stn_site_id = feat['properties']['siteid']
            if stn_site_id == self.station:
                self.station_coords = feat['geometry']['coordinates'][1], feat['geometry']['coordinates'][0]

    def find_closest_station(self, target_lat, target_lon):
        """ The two-argument inverse tangent function.
        :param station_data:
        :param target_lat:
        :param target_lon:
        :return:
        """
        distances = {}
        station_coords = {}
        station_data = self.load_stations()
        for feat in station_data['features']:
            stn_crds = feat['geometry']['coordinates']
            stn_site_id = feat['properties']['siteid']
            lat_stn, lon_stn = stn_crds[1], stn_crds[0]
            dist = geodesic((target_lat, target_lon), (lat_stn, lon_stn)).km
            distances[stn_site_id] = dist
            station_coords[stn_site_id] = lat_stn, lon_stn
        k = min(distances, key=distances.get)
        self.distances = sorted(list(distances.items()), key=lambda x: x[1])
        self.distance_from_station = distances[k]
        self.station_coords = station_coords
        return k

    def load_stations(self):
        r = requests.get(self.station_info_url)
        stations = json.loads(r.text)
        return stations

    def fetch_met_data(self, return_raw=False, out_csv_file=None):

        if self.region == 'pn':
            url = '{}?{}'.format(AGRIMET_MET_REQ_SCRIPT_PN, self.params)
            raw_df = read_csv(url, skip_blank_lines=True,
                              header=0, sep=r'\,|\t', engine='python')

        if self.region == 'gp':
            pairs = ','.join(['{} {}'.format(self.station.upper(), x.upper()) for x in STANDARD_PARAMS])
            url = "https://www.usbr.gov/gp-bin/webarccsv.pl?parameter={0}&syer={1}&smnth={2}&sdy={3}&" \
                  "eyer={4}&emnth={5}&edy={6}&format=2".format(pairs,
                                                               self.start.year,
                                                               self.start.month,
                                                               self.start.day,
                                                               self.end.year,
                                                               self.end.month,
                                                               self.end.day)

            r = requests.get(url)
            raw_df = read_table(io.StringIO(r.content.decode('utf-8')), header=19,
                                sep=',', index_col=0)
            raw_df.rename(str.lower, axis='columns', inplace=True)
            col_names = raw_df.columns.values.tolist()
            if len(col_names) == len(TARGET_COLUMNS):
                col_names = [x.format(a=self.station) for x in TARGET_COLUMNS]
            cols = raw_df.columns[raw_df.dtypes.eq('object')]
            raw_df[cols] = raw_df[cols].apply(to_numeric, errors='coerce')
            raw_df.columns = col_names

        raw_df.index = date_range(self.start, periods=raw_df.shape[0], name='DateTime')
        raw_df = raw_df[to_datetime(self.start): to_datetime(self.end)]

        try:
            raw_df.drop(columns='DateTime', inplace=True)
        except KeyError:
            pass

        if raw_df.shape[0] > 3:
            self.empty_df = False

        if return_raw:
            return raw_df

        raw_df = raw_df[[x.format(a=self.station) for x in TARGET_COLUMNS]]
        reformed_data = self._reformat_dataframe(raw_df)

        if out_csv_file:
            reformed_data.to_csv(path_or_buf=out_csv_file)

        return reformed_data

    def fetch_crop_data(self, out_csv_file=None):

        if not self.start.year == self.end.year:
            raise ValueError('Must choose one year for crop water use reports.')

        if self.region == 'pn':
            # this may need a recursive scheme to go down list of closest stations
            two_dig_yr = format(int(str(self.start.year)[-2:]), '02d')
            url = AGRIMET_CROP_REQ_SCRIPT_PN.format(self.station, two_dig_yr)
            raw_df = read_table(url, skip_blank_lines=True, skiprows=[3], index_col=[0],
                                header=2, engine='python', delim_whitespace=True)
            raw_df = raw_df.iloc[1:, :]
            try:
                start_str = raw_df.first_valid_index().replace('/', '')
            except AttributeError:
                start_str = format(int(raw_df.first_valid_index()), '03d')

        if self.region == 'gp':
            raw_df, start_str = self.get_gp_crop()

        et_summary_start = datetime.strptime('{}{}'.format(self.start.year, start_str), '%Y%m%d')
        raw_df.index = date_range(et_summary_start, periods=raw_df.shape[0])
        idx = date_range(self.start, end=self.end)

        raw_df.replace('--', '0.0', inplace=True)
        cols = raw_df.columns.values.tolist()
        try:
            raw_df = raw_df.astype(float)
        except ValueError:
            raw_df = (raw_df.drop(cols, axis=1).join(raw_df[cols].apply(to_numeric, errors='coerce')))

        raw_df.interpolate(inplace=True)

        reformed_data = raw_df.reindex(idx, fill_value=0.0)
        cols = reformed_data.columns.values.tolist()
        for c in cols:
            reformed_data[c] *= 25.4

        if out_csv_file:
            reformed_data.to_csv(path_or_buf=out_csv_file)

        return reformed_data

    def get_gp_crop(self):
        url = AGRIMET_CROP_REQ_SCRIPT_GP.format(self.station, self.start.year)
        raw_df = read_table(url, skip_blank_lines=True, skiprows=[3], index_col=[0],
                            header=2, engine='python', delim_whitespace=True)

        raw_df = raw_df.iloc[2:, :]
        start_str = format(int(raw_df.first_valid_index()), '03d')
        return raw_df, start_str

    def _reformat_dataframe(self, df):

        old_cols = df.columns.values.tolist()
        head_1 = []
        head_2 = []
        head_3 = []
        for x in old_cols:
            end = x.replace('{}_'.format(self.station), '')
            for j, k, l in WEATHER_PARAMETRS:
                if end.upper() == j.upper():
                    head_1.append(j.upper())
                    head_2.append(k)
                    head_3.append(l)
                    break
        df.columns = [head_1, head_2, head_3]

        for i, col in enumerate(head_1, start=0):
            try:
                # convert to standard units
                if col in ['ET', 'ETRS', 'ETOS', 'PC', 'PP', 'PU']:
                    # in to mm
                    df[col] *= 25.4
                if col in ['MN', 'MX', 'MM', 'YM']:
                    # F to C
                    df[col] = (df[col] - 32) * 5 / 9
                if col in ['UA', 'WG']:
                    # mph to m s-1
                    df[col] *= 0.44704
                if col == 'WR':
                    # mi to m
                    df['WR'] *= 1609.34
                if col == 'SR':
                    # Langleys to W m-2
                    df['SR'] /= 23.900574
            except KeyError:
                head_1.remove(head_1[i])
                head_2.remove(head_2[i])
                head_3.remove(head_3[i])

        df.columns = [head_1, head_2, head_3]

        return df

    @staticmethod
    def write_agrimet_sation_shp(json_data, epsg, out):
        agri_schema = {'geometry': 'Point',
                       'properties': {
                           'program': 'str',
                           'url': 'str',
                           'siteid': 'str',
                           'title': 'str',
                           'state': 'str',
                           'type': 'str',
                           'region': 'str',
                           'install': 'str'}}

        cord_ref = from_epsg(epsg)
        shp_driver = 'ESRI Shapefile'

        with collection(out, mode='w', driver=shp_driver, schema=agri_schema,
                        crs=cord_ref) as output:
            for rec in json_data['features']:
                try:
                    output.write({'geometry': {'type': 'Point',
                                               'coordinates':
                                                   (rec['geometry']['coordinates'][0],
                                                    rec['geometry']['coordinates'][1])},
                                  'properties': {
                                      'program': rec['properties']['program'],
                                      'url': rec['properties']['url'],
                                      'siteid': rec['properties']['siteid'],
                                      'title': rec['properties']['title'],
                                      'state': rec['properties']['state'],
                                      'type': rec['properties']['type'],
                                      'region': rec['properties']['region'],
                                      'install': rec['properties']['install']}})
                except KeyError:
                    pass


if __name__ == '__main__':
    pass

# ========================= EOF ====================================================================
