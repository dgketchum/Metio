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
from calendar import monthrange
from copy import deepcopy
from datetime import datetime
from pprint import pprint

from fiona import collection
from fiona import open as fopen
from fiona.crs import from_epsg
from numpy import nan, empty
from pandas import read_csv, DataFrame, date_range, concat, Series, isnull
from pyproj import Proj
from refet import calcs
from refet.daily import Daily
from sklearn import linear_model

from met.agrimet import Agrimet
from met.elevation import get_elevation
from met.thredds import GridMet

DIVERSIONS = [58387,
              49845,
              46556,
              51841,
              67268,
              54393,
              12506,
              11070,
              9194,
              12749,
              11751,
              13189,
              35088,
              36270,
              34459,
              10669,
              18107,
              15245,
              6891,
              10709,
              9789,
              9651,
              9212,
              6586,
              22281,
              22805,
              23495,
              21634,
              18792,
              0,
              19074,
              0,
              29876,
              20972,
              33081,
              31501,
              45642,
              52686,
              54889,
              39324,
              51549,
              39929,
              51615,
              47737,
              47645,
              44139,
              69787,
              71415,
              31549,
              39940,
              39376,
              10267,
              41650,
              28621,
              172816,
              143366,
              64019,
              109572,
              193022,
              160505,
              19702,
              15757,
              22013,
              18504,
              23927,
              18081,
              258789,
              279622,
              241730,
              253308,
              298819,
              254453,
              10737,
              13888,
              6946,
              17077,
              15632,
              10450,
              26857,
              21223,
              22941,
              25759,
              26060,
              18566,
              27556,
              22717,
              23913,
              29053,
              32409,
              25694,
              361749,
              350892,
              296019,
              197893,
              359238,
              306591, ]

TABLES = ['Broadwater_Missouri_Canal',
          'Broadwater_Missouri_West_Side_Canal',
          'Dodson_North_Canal_Diversion',
          'East_Fork_Main_Canal_ab_Trout_Creek',
          'Eldorado',
          'Floweree_and_Floweree_Hamilton',
          'Fort_Belknap_Main_Diversion',
          'Fort_Shaw_Canal',
          'Glasgow_ID',
          'Huntley_Main_Diversion',
          'Paradise_Valley_ID',
          'Sun_River_project_Below_Pishkun',
          'Two_Dot_Canal',
          'Vigilante_Canal',
          'West_Bench_Canal',
          'Yellowstone_Main_Diversion']

INTERSECTING_HUC_OE = {
    '10030101': ['Broadwater_Missouri_Canal',
                 'Broadwater_Missouri_West_Side_Canal'],
    '17010202': ['East_Fork_Main_Canal_ab_Trout_Creek'],
    '10030205': ['Eldorado'],
    '10030104': ['Floweree_and_Floweree_Hamilton',
                 'Sun_River_project_Below_Pishkun',
                 'Fort_Shaw_Canal'],
    '10050004': ['Fort_Belknap_Main_Diversion',
                 'Paradise_Valley_ID',
                 'Dodson_North_Canal_Diversion'],
    '10050012': ['Glasgow_ID'],
    '10070007': ['Huntley_Main_Diversion'],
    '10040201': ['Two_Dot_Canal'],
    '10020003': ['Vigilante_Canal',
                 'West_Bench_Canal'],
    '10100004': ['Yellowstone_Main_Diversion']
}

NATURAL_SITES = {'Broadwater_Missouri_Canal': (-111.436, 46.330),
                 'Broadwater_Missouri_West_Side_Canal': (-111.52093, 46.19899),
                 'Dodson_North_Canal_Diversion': (-108.09676, 48.38433),
                 'East_Fork_Main_Canal_ab_Trout_Creek': (-113.39298, 46.21571),
                 'Eldorado': (-112.3252, 47.9251),
                 'Floweree_and_Floweree_Hamilton': (-112.0818, 47.5351),
                 'Fort_Belknap_Main_Diversion': (-109.1423, 48.6234),
                 'Fort_Shaw_Canal': (-111.95927, 47.47231),
                 'Glasgow_ID': (-106.73192, 48.2698),
                 'Marshall_Canal': (-113.35616, 46.31922),
                 'Huntley_Main_Diversion': (-108.1154, 46.0094),
                 'Paradise_Valley_ID': (-109.1186, 48.5414),
                 'Sun_River_project_Below_Pishkun': (-111.91403, 47.70035),
                 'Two_Dot_Canal': (-110.0588, 46.45903),
                 'Vigilante_Canal': (-112.0781, 45.3519),
                 'West_Bench_Canal': (-112.1632, 45.34213),
                 'Yellowstone_Main_Diversion': (-104.2127, 47.71017)}

HUC_TABLES = [
    'MT_10010002',
    'MT_10020001',
    'MT_10020002',
    'MT_10020003',
    'MT_10020004',
    'MT_10020005',
    'MT_10020006',
    'MT_10020007',
    'MT_10020008',
    'MT_10030101',
    'MT_10030102',
    'MT_10030103',
    'MT_10030104',
    'MT_10030105',
    'MT_10030201',
    'MT_10030202',
    'MT_10030203',
    'MT_10030204',
    'MT_10030205',
    'MT_10040101',
    'MT_10040102',
    'MT_10040103',
    'MT_10040104',
    'MT_10040105',
    'MT_10040106',
    'MT_10040201',
    'MT_10040202',
    'MT_10040203',
    'MT_10040204',
    'MT_10040205',
    'MT_10050001',
    'MT_10050002',
    'MT_10050003',
    'MT_10050004',
    'MT_10050005',
    'MT_10050006',
    'MT_10050007',
    'MT_10050008',
    'MT_10050009',
    'MT_10050010',
    'MT_10050011',
    'MT_10050012',
    'MT_10050013',
    'MT_10050014',
    'MT_10050015',
    'MT_10050016',
    'MT_10060001',
    'MT_10060002',
    'MT_10060003',
    'MT_10060004',
    'MT_10060005',
    'MT_10060006',
    'MT_10060007',
    'MT_10070001',
    'MT_10070002',
    'MT_10070003',
    'MT_10070004',
    'MT_10070005',
    'MT_10070006',
    'MT_10070007',
    'MT_10070008',
    'MT_10080010',
    'MT_10080014',
    'MT_10080015',
    'MT_10080016',
    'MT_10090101',
    'MT_10090102',
    'MT_10090207',
    'MT_10090208',
    'MT_10090209',
    'MT_10090210',
    'MT_10100001',
    'MT_10100002',
    'MT_10100003',
    'MT_10100004',
    'MT_10100005',
    'MT_10110201',
    'MT_10110202',
    'MT_10110204',
    'MT_17010101',
    'MT_17010102',
    'MT_17010104',
    'MT_17010201',
    'MT_17010202',
    'MT_17010203',
    'MT_17010204',
    'MT_17010205',
    'MT_17010206',
    'MT_17010208',
    'MT_17010210',
    'MT_17010211',
    'MT_17010212',
    'MT_17010213'
]

HUC_TABLES_FOUND = [
    # 'MT_10010002',
    'MT_10020001',
    'MT_10020002',
    # 'MT_10020003',
    # 'MT_10020004',
    # 'MT_10020005',
    # 'MT_10020006',
    # 'MT_10020007',
    # 'MT_10020008',
    # 'MT_10030101',
    # 'MT_10030102',
    # 'MT_10030103',
    # 'MT_10030104',
    # 'MT_10030105',
    # 'MT_10030201',
    # 'MT_10030202',
    # 'MT_10030203',
    # 'MT_10030204',
    # 'MT_10030205',
    # 'MT_10040101',
    # 'MT_10040102',
    # 'MT_10040103',
    # 'MT_10040104',
    # 'MT_10040105',
    # 'MT_10040106',
    # 'MT_10040201',
    # 'MT_10040202',
    # 'MT_10040203',
    # 'MT_10040204',
    # 'MT_10040205',
    # 'MT_10050001',
    # 'MT_10050002',
    # 'MT_10050003',
    # 'MT_10050004',
    # 'MT_10050005',
    # 'MT_10050006',
    # 'MT_10050007',
    # 'MT_10050008',
    # 'MT_10050009',
    # 'MT_10050010',
    # 'MT_10050011',
    # 'MT_10050012',
    # 'MT_10050013',
    # 'MT_10050014',
    # 'MT_10050015',
    # 'MT_10050016',
    # 'MT_10060001',
    # 'MT_10060002',
    # 'MT_10060003',
    # 'MT_10060004',
    # 'MT_10060005',
    # 'MT_10060006',
    # 'MT_10060007',
    # 'MT_10070001',
    # 'MT_10070002',
    # 'MT_10070003',
    # 'MT_10070004',
    # 'MT_10070005',
    # 'MT_10070006',
    # 'MT_10070007',
    # 'MT_10070008',
    # 'MT_10080010',
    # 'MT_10080014',
    # 'MT_10080015',
    # 'MT_10080016',
    # 'MT_10090101',
    # 'MT_10090102',
    # 'MT_10090207',
    # 'MT_10090208',
    # 'MT_10090209',
    # 'MT_10090210',
    # 'MT_10100001',
    # 'MT_10100002',
    # 'MT_10100003',
    # 'MT_10100004',
    # 'MT_10100005',
    # 'MT_10110201',
    # 'MT_10110202',
    # 'MT_10110204',
    # 'MT_17010101',
    # 'MT_17010102',
    # 'MT_17010104',
    # 'MT_17010201',
    # 'MT_17010202',
    # 'MT_17010203',
    # 'MT_17010204',
    # 'MT_17010205',
    # 'MT_17010206',
    # 'MT_17010208',
    # 'MT_17010210',
    # 'MT_17010211',
    # 'MT_17010212',
    # 'MT_17010213'
]

COUNTIES = [
    'BE',
    'BH',
    'BL',
    'BR',
    'CA',
    'CH',
    'CR',
    'CS',
    'CU',
    'DA',
    'DL',
    'DW',
    'FA',
    'FE',
    'FL',
    'GA',
    'GF',
    'GL',
    'GR',
    'GV',
    'HI',
    'JB',
    'JE',
    'LA',
    'LC',
    'LI',
    'LN',
    'MA',
    'MC',
    'ME',
    'MI',
    'MS',
    'MU',
    'PA',
    'PE',
    'PH',
    'PI',
    'PO',
    'PR',
    'PW',
    'RA',
    'RI',
    'RO',
    'RS',
    'SA',
    'SB',
    'SG',
    'SH',
    'ST',
    'TE',
    'TO',
    'TR',
    'VA',
    'WH',
    'WI',
    'YE'
]

COUNTY_KEY = {
    'CA': 'CARBON',
    'PR': 'POWDER_RIVER',
    'MA': 'MADISON',
    'BE': 'BEAVERHEAD',
    'BH': 'BIG_HORN',
    'ST': 'STILLWATER',
    'PA': 'PARK',
    'GA': 'GALLATIN',
    'SG': 'SWEET_GRASS',
    'SB': 'SILVER_BOW',
    'CR': 'CARTER',
    'DL': 'DEER_LODGE',
    'TR': 'TREASURE',
    'YE': 'YELLOWSTONE',
    'JE': 'JEFFERSON',
    'GV': 'GOLDEN_VALLEY',
    'WH': 'WHEATLAND',
    'RA': 'RAVALLI',
    'MU': 'MUSSELSHELL',
    'FA': 'FALLON',
    'BR': 'BROADWATER',
    'RS': 'ROSEBUD',
    'GR': 'GRANITE',
    'CU': 'CUSTER',
    'ME': 'MEAGHER',
    'PI': 'PRAIRIE',
    'JB': 'JUDITH_BASIN',
    'WI': 'WIBAUX',
    'PE': 'PETROLEUM',
    'MI': 'MINERAL',
    'PW': 'POWELL',
    'MS': 'MISSOULA',
    'CS': 'CASCADE',
    'FE': 'FERGUS',
    'DW': 'DAWSON',
    'LC': 'LEWIS & CLARK',
    'GF': 'GARFIELD',
    'LA': 'LAKE',
    'MC': 'MCCONE',
    'TE': 'TETON',
    'RI': 'RICHLAND',
    'CH': 'CHOUTEAU',
    'SA': 'SANDERS',
    'PO': 'PONDERA',
    'RO': 'ROOSEVELT',
    'HI': 'HILL',
    'BL': 'BLAINE',
    'LI': 'LIBERTY',
    'PH': 'PHILLIPS',
    'TO': 'TOOLE',
    'VA': 'VALLEY',
    'DA': 'DANIELS',
    'GL': 'GLACIER',
    'FL': 'FLATHEAD',
    'SH': 'SHERIDAN',
    'LN': 'LINCOLN', }

I_TYPES = ['P', 'S', 'F']
YEARS = ['2008', '2009', '2010', '2011', '2012', '2013']

START, END = '{}-04-15', '{}-10-15'
M_START, M_END = '{}-04-01', '{}-10-31'
FMT = '%Y-%m-%d'

D = 3.0


def natural_sites_shp(out_loc):
    agri_schema = {'geometry': 'Point',
                   'properties': {
                       'Name': 'str'}}

    shp_driver = 'ESRI Shapefile'
    epsg = from_epsg(4326)

    with collection(os.path.join(out_loc, 'Natural_Sites.shp'), mode='w',
                    driver=shp_driver, schema=agri_schema, crs=epsg) as output:
        for key, val in NATURAL_SITES.items():
            try:
                output.write({'geometry': {'type': 'Point',
                                           'coordinates':
                                               (val[0], val[1])},
                              'properties': {
                                  'Name': key}})
            except KeyError:
                pass


def effective_precip(precip, ref_et):
    """ National Engineering Handbook method for finding effective precipitation.
    :param ppt:
    :param etc:
    :return:
    """
    ppt = precip / 25.4
    etc = ref_et / 25.4
    sf = (0.531747 + 0.295164 * D - 0.057697 * (D ** 2) + 0.003804 * (D ** 3))
    eff_ppt = sf * (((0.70917 * ppt) ** 0.82416) - 0.11556) * (10 ** (0.02426 * etc))
    eff_ppt_mm = eff_ppt * 25.4
    return eff_ppt_mm


def make_tables_annual(source, root):
    df = None
    for t in source:
        for yr in YEARS:
            f_name = '{}_{}ee_export.csv'.format(t, yr)
            new_f_name = '{}.csv'.format(t)
            csv = os.path.join(root, f_name)
            new_csv = os.path.join(root, new_f_name)
            if yr == '2008':
                df = read_csv(csv)
                to_drop = ['system:index', 'Shape_Leng', '.geo']
                df.drop(columns=to_drop, inplace=True)
                df.rename(columns={'mean': 'mean_{}'.format(yr)}, inplace=True)
                df.rename(columns={'Shape_Area': 'Sq_Meters'}, inplace=True)
            else:
                dummy_df = read_csv(csv)
                s = Series(dummy_df['mean'], name='mean_{}'.format(yr))
                df['mean_{}'.format(yr)] = s
                s = None
        # df.to_csv(new_csv, index_label='ID')


def make_tables_monthly(source, root):
    df = None
    found = []
    for t in source:
        first = True
        try:
            for yr in YEARS:
                for mo in range(4, 11):
                    f_name = '{}_{}_{}ee_export.csv'.format(t, yr, mo)
                    new_f_name = '{}.csv'.format(t)
                    csv = os.path.join(root, f_name)
                    new_csv = os.path.join(root, new_f_name)
                    if first:
                        df = read_csv(csv)
                        to_drop = ['system:index', 'Shape_Leng', '.geo']
                        df.drop(columns=to_drop, inplace=True)
                        df.rename(columns={'mean': 'mean_{}_{}'.format(yr, mo)}, inplace=True)
                        df.rename(columns={'Shape_Area': 'Sq_Meters'}, inplace=True)
                        first = False
                    else:
                        dummy_df = read_csv(csv)
                        s = Series(dummy_df['mean'], name='mean_{}_{}'.format(yr, mo))
                        df['mean_{}_{}'.format(yr, mo)] = s
                        s = None

        except FileNotFoundError:
            pass

        found.append(t)
        df.to_csv(new_csv, index_label='ID')
    pprint(found)


def count_project_fields(source, tables):
    ct = 0
    for t in tables:
        shp = os.path.join(source, '{}.shp'.format(t))
        with fopen(shp, 'r') as src:
            print(shp)
            sub_ct = len([x for x in src])
            ct += sub_ct
            print(sub_ct)

    print(ct)


class DataCollector(Agrimet):

    def __init__(self, project, csv, table, station=None, lat=None, lon=None, monthly=False):
        Agrimet.__init__(self, station=station, lat=lat, lon=lon)

        self.monthly = monthly
        self.make_empty_df()
        self.first = True
        self.project = project
        self.csv = csv
        self.table = table
        self.lat = lat
        self.lon = lon
        self.elev = None
        self.station_rank = 0

    def get_gridmet(self, start, end, lat, lon):
        #  gridmet params
        gridmet = GridMet('pr', start=start, end=end, lat=lat, lon=lon)
        ts_ppt = gridmet.get_point_timeseries()
        m_ppt = ts_ppt.groupby(lambda x: x.month).sum().values
        gridmet = GridMet('etr', start=start, end=end, lat=lat, lon=lon)
        ts_etr = gridmet.get_point_timeseries()
        m_etr = ts_etr.groupby(lambda x: x.month).sum().values
        return m_ppt, m_etr

    def get_agrimet_etr(self, yr, first=False):

        agrimet = Agrimet(station=self.station, start_date=START.format(yr),
                          end_date=END.format(yr), interval='daily')

        formed = agrimet.fetch_met_data()
        if isnull(formed['ETRS']).values.sum() == formed['ETRS'].shape[0]:
            agri_etr = formed['ETRS'].groupby(lambda x: x.month).sum().values
        else:
            agri_etr = formed['ETRS'].groupby(lambda x: x.month).sum().values

        if first:
            self.elev = get_elevation(agrimet.station_coords[0], agrimet.station_coords[1])
            print('Station {}'.format(self.station))

        if formed['YM'].values.mean() != nan:
            t_dew = formed['YM'].values
            ea = calcs._sat_vapor_pressure(t_dew)[0]
            tmin, tmax = formed['MN'].values, formed['MX'].values

            try:
                doy = formed.index.strftime('%j').astype(int).values
            except AttributeError:
                doy = formed.index.strftime('%j').astype(int)

            doy = doy.reshape((len(doy), 1))[0]
            lat = agrimet.station_coords[0]
            rs = formed['SR'].values
            uz = formed['UA'].values
            zw = 2.0
            formed['calc_etr'] = Daily(tmin=tmin, tmax=tmax, ea=ea, rs=rs, uz=uz, zw=zw, elev=self.elev,
                                       lat=lat, doy=doy).etr()
            calc_etr = formed['calc_etr'].groupby(lambda x: x.month).sum().values

        else:
            calc_etr = empty(agri_etr.shape)
            calc_etr[:] = 0.0

        return agri_etr, calc_etr

    def get_agrimet_crop(self, yr):
        agrimet = Agrimet(station=self.station, start_date=START.format(yr),
                          end_date=END.format(yr), interval='daily')
        data = agrimet.fetch_crop_data()
        alfalfa = data['ALFM']
        m_alfalfa = alfalfa.groupby(lambda x: x.month).sum().values
        m_alfalfa = m_alfalfa.reshape((m_alfalfa.shape[0], 1))
        return m_alfalfa

    def count_irrigation_types(self, data):
        count = self.csv.shape[0]
        try:
            p = self.csv.IType.value_counts()['P'] / float(count)
        except KeyError:
            p = 0.0
        except AttributeError:
            p = 'UNK'
        data.append(p)

        try:
            s = self.csv.IType.value_counts()['S'] / float(count)
        except KeyError:
            s = 0.0
        except AttributeError:
            s = 'UNK'
        data.append(s)

        try:
            f = self.csv.IType.value_counts()['F'] / float(count)
        except KeyError:
            f = 0.0
        except AttributeError:
            f = 'UNK'
        data.append(f)

        return data

    def make_empty_df(self):
        if self.monthly:
            index = date_range(start='20080101', end='20131231', freq='m')
        else:
            index = date_range(start='20080101', end='20131231', freq='y')
        df = DataFrame(data=None, columns=['name',
                                           'code',
                                           'gridmet_ppt',
                                           'gridmet_etr',
                                           'agrimet_etr_pm_provided',
                                           'agrimet_etr_calculated',
                                           'ratio_agrimet_to_gridmet',
                                           'eff_ppt_crop_coef',
                                           'gridmet_eff_ppt',
                                           'agrimet_eff_ppt',
                                           'Acres_Tot',
                                           'Acres_Project',
                                           'Projects',
                                           'Sq_Meters',
                                           'Acres_Irr',
                                           'Sq_Meters_Irr',
                                           'Weighted_Mean_ET_mm',
                                           'Crop_Cons_mm',
                                           'ET_m3',
                                           'ET_af',
                                           'Crop_Cons_m3',
                                           'Crop_Cons_af',
                                           'pivot',
                                           'sprinkler',
                                           'flood'],
                       index=index)
        self.df = df

    def get_table_data_monthly(self):
        for yr in YEARS:
            if yr == YEARS[0]:
                first = True
            else:
                first = False

            if self.project == 'co':
                data = [COUNTY_KEY[self.table], self.table]
            elif self.project == 'huc':
                data = [self.table.replace('MT_', ''), None]
            else:
                data = [self.table, None]

            s, e = datetime.strptime(START.format(yr), FMT), datetime.strptime(END.format(yr), FMT)
            lat, lon = self.station_coords[self.station][0], self.station_coords[self.station][1]
            m_ppt, m_etr = self.get_gridmet(s, e, lat, lon)
            m_agri_etr, calc_etr = self.get_agrimet_etr(yr, first=first)
            m_crop_use = self.get_agrimet_crop(yr)

            m_gridmet_eff_ppt = effective_precip(m_ppt, m_etr)
            m_agrimet_eff_ppt = effective_precip(m_ppt, m_agri_etr)
            m_crop_use_eff_ppt = effective_precip(m_ppt, m_crop_use)

            ratio = calc_etr[:] / m_etr[:, 0]

            for month, i in zip(range(4, 11), range(0, 7)):

                summary = deepcopy(data)
                [summary.append(x) for x in [m_ppt[i][0], m_etr[i][0], m_agri_etr[i][0], calc_etr[i],
                                             ratio[i], m_crop_use_eff_ppt[i][0],
                                             m_gridmet_eff_ppt[i][0], m_agrimet_eff_ppt[i][0]]]

                days = monthrange(int(yr), month=month)[1]
                dt = datetime(int(yr), month, days)
                mean_key = 'mean_{}_{}'.format(yr, month)

                try:
                    acres_tot = self.csv['ACRES'].values.sum()
                except KeyError:
                    acres_tot = self.csv['Acres'].values.sum()

                if self.project == 'huc' and 'WUDR OE Proj' in list(self.csv['UNIT'].unique()):
                    print('{} GIS: {}'.format(self.table, self.csv['UNIT'].unique()))
                    proj_df = self.csv[self.csv['UNIT'] == 'WUDR OE Proj']
                    try:
                        acres_proj = proj_df['ACRES'].values.sum()
                    except KeyError:
                        acres_proj = proj_df['Acres'].values.sum()
                    acres_tot -= acres_proj
                else:
                    acres_proj = 0.0

                if self.table in INTERSECTING_HUC_OE.keys():
                    intersect_project = INTERSECTING_HUC_OE[self.table]
                else:
                    intersect_project = None

                acres_irr = acres_tot
                sq_m_tot = self.csv['Sq_Meters'].values.sum()
                sq_m_irr = sq_m_tot
                [summary.append(x) for x in [acres_tot, acres_proj, intersect_project, sq_m_tot, acres_irr, sq_m_irr]]

                mean_mm = (self.csv[mean_key] * self.csv['Sq_Meters'] / self.csv['Sq_Meters'].values.sum()).values.sum()
                et_vol_yr_m3 = (self.csv['Sq_Meters'] * self.csv[mean_key] / 1000.).values.sum()
                et_vol_yr_af = (self.csv['Sq_Meters'] * self.csv[mean_key] / (1000. * 1233.48)).values.sum()
                cc_vol_yr_cm = (
                        self.csv['Sq_Meters'] * (self.csv[mean_key] - m_crop_use_eff_ppt[i]) / 1000.).values.sum()
                cc_vol_yr_af = (
                        self.csv['Sq_Meters'] * (self.csv[mean_key] - m_crop_use_eff_ppt[i]) / (
                        1000. * 1233.48)).values.sum()

                cc_mean_mm = mean_mm - m_crop_use_eff_ppt[i]
                [summary.append(x) for x in [mean_mm, cc_mean_mm[0], et_vol_yr_m3,
                                             et_vol_yr_af, cc_vol_yr_cm, cc_vol_yr_af]]

                summary = self.count_irrigation_types(summary)
                self.df.loc[dt] = summary

            self.check_area(acres_tot, sq_m_tot)

    def get_table_data_annual(self):
        try:
            for yr in YEARS:

                if yr == YEARS[0]:
                    first = True
                else:
                    first = False

                if self.project == 'co':
                    data = [COUNTY_KEY[self.table], self.table]
                elif self.project == 'huc':
                    data = [self.table.replace('MT_', ''), None]
                else:
                    data = [self.table, None]

                s, e = datetime.strptime(START.format(yr), FMT), datetime.strptime(END.format(yr), FMT)
                lat, lon = self.station_coords[self.station][0], self.station_coords[self.station][1]
                m_ppt, m_etr = self.get_gridmet(s, e, lat, lon)
                m_agri_etr, calc_etr = self.get_agrimet_etr(yr, first=first)
                m_crop_use = self.get_agrimet_crop(yr)

                m_gridmet_eff_ppt = effective_precip(m_ppt, m_etr)
                m_agrimet_eff_ppt = effective_precip(m_ppt, m_agri_etr)
                m_crop_use_eff_ppt = effective_precip(m_ppt, m_crop_use)

                s_grid_eff_ppt = m_gridmet_eff_ppt.sum()
                s_agri_eff_ppt = m_agrimet_eff_ppt.sum()
                s_crop_coef_eff_ppt = m_crop_use_eff_ppt.sum()

                season_agri_etr = m_agri_etr.sum()
                season_agri_etr_calc = calc_etr.sum()
                season_ppt, season_grid_etr = m_ppt.sum(), m_etr.sum(),
                ratio = season_agri_etr / season_grid_etr
                if ratio > 10:
                    ratio = season_agri_etr_calc / season_grid_etr

                [data.append(x) for x in [season_ppt, season_grid_etr, season_agri_etr, season_agri_etr_calc,
                                          ratio, s_crop_coef_eff_ppt,
                                          s_grid_eff_ppt, s_agri_eff_ppt]]
                if self.project == 'oe':
                    self.oe_project_summary(yr, s_crop_coef_eff_ppt, data)

                elif self.project in ['huc', 'co']:
                    self.project_summary(yr, s_crop_coef_eff_ppt, data)

                else:
                    Exception('Choose a valid project type.')

        except Exception as e:
            print('Error on station {}: {}'.format(self.station, e))
            self.station_rank += 1
            self.station = self.distances[self.station_rank][0]
            self.get_table_data()

    def project_summary(self, yr, season_eff_ppt, data_list):
        dt = datetime(int(yr), 12, 31)
        mean_key = 'mean_{}'.format(yr)

        try:
            acres_tot = self.csv['ACRES'].values.sum()
        except KeyError:
            acres_tot = self.csv['Acres'].values.sum()

        if self.project == 'huc' and 'WUDR OE Proj' in list(self.csv['UNIT'].unique()):
            print('{} GIS: {}'.format(self.table, self.csv['UNIT'].unique()))
            proj_df = self.csv[self.csv['UNIT'] == 'WUDR OE Proj']
            try:
                acres_proj = proj_df['ACRES'].values.sum()
            except KeyError:
                acres_proj = proj_df['Acres'].values.sum()
            acres_tot -= acres_proj
        else:
            acres_proj = 0.0

        if self.table in INTERSECTING_HUC_OE.keys():
            intersect_project = INTERSECTING_HUC_OE[self.table]
        else:
            intersect_project = None

        acres_irr = acres_tot
        sq_m_tot = self.csv['Sq_Meters'].values.sum()
        sq_m_irr = sq_m_tot
        [data_list.append(x) for x in [acres_tot, acres_proj, intersect_project, sq_m_tot, acres_irr, sq_m_irr]]

        mean_mm = (self.csv[mean_key] * self.csv['Sq_Meters'] / self.csv['Sq_Meters'].values.sum()).values.sum()
        et_vol_yr_m3 = (self.csv['Sq_Meters'] * self.csv[mean_key] / 1000.).values.sum()
        et_vol_yr_af = (self.csv['Sq_Meters'] * self.csv[mean_key] / (1000. * 1233.48)).values.sum()
        cc_vol_yr_cm = (self.csv['Sq_Meters'] * (self.csv[mean_key] - season_eff_ppt) / 1000.).values.sum()
        cc_vol_yr_af = (
                self.csv['Sq_Meters'] * (self.csv[mean_key] - season_eff_ppt) / (1000. * 1233.48)).values.sum()

        cc_mean_mm = mean_mm - season_eff_ppt
        [data_list.append(x) for x in [mean_mm, cc_mean_mm, et_vol_yr_m3,
                                       et_vol_yr_af, cc_vol_yr_cm, cc_vol_yr_af]]

        #  irrigation types

        data_list = self.count_irrigation_types(data_list)
        self.df.loc[dt] = data_list

        self.check_area(acres_tot, sq_m_tot)

    def oe_project_summary(self, yr, season_eff_ppt, data_list):
        dt = datetime(int(yr), 12, 31)
        mean_key = 'mean_{}'.format(yr)
        irr_key = 'Irr_{}'.format(yr)
        irr_df = self.csv[self.csv[irr_key] == 1]
        try:
            acres_tot = self.csv['Acres'].values.sum()
            acres_irr = irr_df['Acres'].values.sum()
            area_check = irr_df['Acres'].values
        except KeyError:
            try:
                acres_tot = self.csv['ACRES'].values.sum()
                acres_irr = irr_df['ACRES'].values.sum()
                area_check = irr_df['ACRES'].values
            except KeyError:
                acres_tot = self.csv['acres'].values.sum()
                acres_irr = irr_df['acres'].values.sum()
                area_check = irr_df['acres'].values
                # for the case of Yellowstone has erroneous Sq Meters
                # irr_df['Sq_Meters'] = self.csv['acres'] * 4046.86

        sq_m_irr = irr_df['Sq_Meters'].values.sum()
        sq_m_tot = self.csv['Sq_Meters'].values.sum()
        [data_list.append(x) for x in [acres_tot, sq_m_tot, acres_irr, sq_m_irr]]

        # irrigation volumes
        mean_mm = (self.csv[mean_key] * self.csv['Sq_Meters'] / self.csv['Sq_Meters'].values.sum()).values.sum()
        cc_mean_mm = mean_mm - season_eff_ppt
        et_vol_yr_m3 = (irr_df['Sq_Meters'] * irr_df[mean_key] / 1000.).values.sum()
        et_vol_yr_af = (irr_df['Sq_Meters'] * irr_df[mean_key] / (1000. * 1233.48)).values.sum()
        cc_vol_yr_cm = (irr_df['Sq_Meters'] * (irr_df[mean_key] - season_eff_ppt) / 1000.).values.sum()
        cc_vol_yr_af = (
                irr_df['Sq_Meters'] * (irr_df[mean_key] - season_eff_ppt) / (1000. * 1233.48)).values.sum()
        [data_list.append(x) for x in [mean_mm, cc_mean_mm, et_vol_yr_m3,
                                       et_vol_yr_af, cc_vol_yr_cm, cc_vol_yr_af]]

        data_list = self.count_irrigation_types(data_list)
        self.df.loc[dt] = data_list
        self.check_area(acres_tot, sq_m_tot)

    @staticmethod
    def check_area(acres, sq_meters):
        try:
            diff = abs(acres - (sq_meters / 4046.86)) / acres
            assert diff < 0.01
        except AssertionError:
            print('Area check: {} acres should be {} '
                  'sq m, actual is {} sq m'.format(acres,
                                                   acres * 4046.86,
                                                   sq_meters))


def build_summary_table_monthly(source, shapes, tables, out_loc, project='oe'):
    master = DataFrame()
    lat, lon = None, None
    for table in source:
        print('Processing {}'.format(table))
        try:
            csv = read_csv(os.path.join(tables, '{}.csv'.format(table)))

            shp = os.path.join(shapes, '{}.shp'.format(table))
            with fopen(shp) as src:
                # .shp files should be in epsg: 102300
                for feat in src:
                    if src.crs != {'init': 'epsg:4326'}:
                        coords = feat['geometry']['coordinates'][0][0]
                        if len(coords) > 2:
                            coords = coords[0]

                        lat, lon = state_plane_MT_to_WGS(coords[1], coords[0])
                        break
                    else:
                        lat, lon = feat['geometry']['coordinates'][1], feat['geometry']['coordinates'][0]

            d = DataCollector(project=project, csv=csv, table=table, lat=lat, lon=lon, monthly=True)
            d.get_table_data_monthly()
            d.df.dropna(axis=0, how='all', inplace=True)
            master = concat([master, d.df])

        except FileNotFoundError:
            print('{} not found'.format(table))

    if project == 'oe':
        master['DIVERSIONS'] = DIVERSIONS
        master = master[master['DIVERSIONS'] > 0.]
        master['EFF'] = master['Crop_Cons_af'] / master['DIVERSIONS']

    master.to_csv(os.path.join(out_loc, 'HUC_8_Monthly.csv'), date_format='%Y-%m')


def build_summary_table(source, shapes, tables, out_loc, project='oe'):
    master = DataFrame()
    lat, lon = None, None
    for table in source:
        print('Processing {}'.format(table))
        try:
            csv = read_csv(os.path.join(tables, '{}.csv'.format(table)))

            shp = os.path.join(shapes, '{}.shp'.format(table))
            with fopen(shp) as src:
                # .shp files should be in epsg: 102300
                for feat in src:
                    if src.crs != {'init': 'epsg:4326'}:
                        coords = feat['geometry']['coordinates'][0][0]
                        if len(coords) > 2:
                            coords = coords[0]

                        lat, lon = state_plane_MT_to_WGS(coords[1], coords[0])
                        break
                    else:
                        lat, lon = feat['geometry']['coordinates'][1], feat['geometry']['coordinates'][0]

            d = DataCollector(project=project, csv=csv, table=table, lat=lat, lon=lon)
            d.get_table_data()

            master = concat([master, d.df])

        except FileNotFoundError:
            print('{} not found'.format(table))

    if project == 'oe':
        master['DIVERSIONS'] = DIVERSIONS
        master = master[master['DIVERSIONS'] > 0.]
        master['EFF'] = master['Crop_Cons_af'] / master['DIVERSIONS']

    master.to_csv(os.path.join(out_loc, 'HUC_8_wProjects.csv'), date_format='%Y')


def state_plane_MT_to_WGS(y, x):
    # 102300
    nad83_MT = '+proj=lcc +lat_1=45 +lat_2=49 +lat_0=44.25 +lon_0=-109.5 ' \
               '+x_0=600000 +y_0=0 +ellps=GRS80 +units=m +no_defs'
    # 32100
    nad83_HARN_SP_MT = '+proj=lcc +lat_1=49 +lat_2=45 +lat_0=44.25 +lon_0=-109.5 +x_0=600000 ' \
                       '+y_0=0 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs'

    in_proj = Proj(nad83_HARN_SP_MT, preserve_units=True)
    x, y = in_proj(x, y, inverse=True)

    return y, x


class Withdrawals(object):

    def __init__(self, diversion_data, project_et, statewide_et):
        self.diversions = read_csv(diversion_data, header=0, index_col=0)
        self.project_et = read_csv(project_et, header=0, index_col=0)
        self.et = read_csv(statewide_et, header=0)
        self.df_oe = DataFrame()

        self.model = None
        self.score = None
        self.lm = None

    def get_efficiency(self):
        df = concat([self.diversions, self.project_et], axis=1)
        self.df_oe = df
        self.df_oe['efficiency'] = df['Crop_Cons_af'] / df['DIVERSION']
        self.df_oe.drop(columns=['name'], inplace=True)

    def find_regression(self, csv=None):
        if self.df_oe.empty:
            self.get_efficiency()

        self.df_oe = self.df_oe[self.df_oe['efficiency'] < 1.]

        x = self.df_oe.loc[:, 'pivot':'flood'].values
        y = self.df_oe.loc[:, 'efficiency'].values
        y = y.reshape((y.shape[0], 1))
        self.lm = linear_model.LinearRegression()
        self.model = self.lm.fit(x, y)
        pred = self.lm.predict(x)
        self.score = self.lm.score(x, y)
        self.df_oe['predicted_efficiency'] = pred
        if csv:
            self.df_oe.to_csv(csv)

    def predict_withdrawals(self, csv=None, predict=False, **eff_values):

        x = self.et.loc[:, 'pivot':'flood'].values
        if predict:
            pred = self.lm.predict(x)
            self.et['predicted_efficiency'] = pred

            cc = self.et['Crop_Cons_m3'].values
            self.et['total_withdrawal_lr_m3'] = 1 / pred * cc.reshape((cc.shape[0], 1))

            cc = self.et['Crop_Cons_af'].values
            self.et['total_withdrawal_lr_af'] = 1 / pred * cc.reshape((cc.shape[0], 1))

        if eff_values:
            self.et['wp'] = self.et['pivot'].values * self.et['Crop_Cons_m3'].values / eff_values['pivot']
            self.et['ws'] = self.et['sprinkler'].values * self.et['Crop_Cons_m3'].values / eff_values['sprinkler']
            self.et['wf'] = self.et['flood'].values * self.et['Crop_Cons_m3'].values / eff_values['flood']
            self.et['total_withdrawal_m3'] = self.et['wp'] + self.et['wf'] + self.et['ws']
            self.et['total_withdrawal_af'] = self.et['total_withdrawal_m3'] / 1233.48

        if csv:
            self.et.to_csv(csv)


# def count_histogram(csv_path):
#     yrs = [str(x) for x in range(2008, 2014, 1)]
#     csv = read_csv(csv_path)
#     # series = [(csv[yr] * csv['Frequency']).sum() for yr in yrs]
#     # tots = [(yr, s/1000000.) for yr, s in zip(yrs, series)]
#     bins = csv['Frequency']
#     # plt.bar(bins, csv['2008'].values, alpha=0.5, label='2008')
#     # plt.bar(bins, csv['2009'].values, alpha=0.5, label='2009')
#     # plt.bar(bins, csv['2010'].values, alpha=0.5, label='2010')
#     # plt.bar(bins, csv['2011'].values, alpha=0.5, label='2011')
#     plt.bar(bins, csv['2012'].values, alpha=0.5, label='2012')
#     plt.bar(bins, csv['2013'].values, alpha=0.5, label='2013')
#     plt.xlabel('Valid Pixels During Growing Season')
#     plt.ylabel('Pixel Locations')
#     plt.legend(loc='upper right')
#     plt.savefig(os.path.join(os.path.dirname(csv_path), '2012-13 hist.png'), bbox_inches='tight')
#     plt.show()


def statewide_annual_statistics(csv, out):
    c = read_csv(csv, index_col=0)
    c['ppt_af'] = c['Acres_Tot'] * c['gridmet_ppt'] / 304.8
    yr = c.groupby('Unnamed: 0.1').agg({'ET_af': 'sum',
                                        'ppt_af': 'sum',
                                        'Crop_Cons_af': 'sum',
                                        'total_withdrawal_af': 'sum'})
    yr.to_csv(out)


if __name__ == '__main__':
    home = os.path.expanduser('~')
    huc_shapefile = os.path.join(home, 'IrrigationGIS', 'Montana',
                                 'Statewide_Irrigation_Shapefile', 'by_huc_8')

    table = os.path.join(home, 'ssebop', 'ssebop_exports', 'huc8_monthly')
    # make_tables_monthly(HUC_TABLES, table)
    build_summary_table_monthly(HUC_TABLES_FOUND, huc_shapefile, table, out_loc=table, project='huc')

# ========================= EOF ====================================================================
