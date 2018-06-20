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
from pandas import read_csv, DataFrame, date_range, concat, Series
from fiona import open as fopen
from fiona import collection
from fiona.crs import from_epsg
from pyproj import Proj
from datetime import datetime

from met.thredds import GridMet
from met.agrimet import Agrimet

TABLES = ['Broadwater_Missouri_Canal',
          'Broadwater_Missouri_West_Side_Canal',
          'Dodson_North_Canal_Diversion',
          'East_Fork_Main_Canal_ab_Trout_Creek',
          'Eldorado',
          'Floweree_and_Floweree_Hamilton',
          'Fort_Belknap_Main_Diversion',
          'Fort_Shaw_Canal',
          'Glasgow_ID',
          'Marshall_Canal',
          'Huntley_Main_Diversion',
          'Paradise_Valley_ID',
          'Sun_River_project_Below_Pishkun',
          'Two_Dot_Canal',
          'Vigilante_Canal',
          'West_Bench_Canal',
          'Yellowstone_Main_Diversion']

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

I_TYPES = ['P', 'S', 'F']
YEARS = ['2009', '2010', '2011', '2012', '2013']

START, END = '{}-04-15', '{}-10-15'
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


def make_tables(root):
    df = None
    for t in TABLES:
        for yr in YEARS:
            f_name = '{}_{}ee_export.csv'.format(t, yr)
            new_f_name = '{}.csv'.format(t)
            csv = os.path.join(root, 'IrrigationGIS', 'ssebop_exports', f_name)
            new_csv = os.path.join(root, 'IrrigationGIS', 'ssebop_exports', new_f_name)
            if yr == '2009':
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
        df.to_csv(new_csv, index_label='ID')


def build_summary_table(shapes, tables, out_loc):
    master = DataFrame()
    for table in TABLES:
        print('Processing {}'.format(table))
        csv = read_csv(os.path.join(tables, '{}.csv'.format(table)))

        shp = os.path.join(shapes, '{}.shp'.format(table))
        with fopen(shp) as src:
            for feat in src:
                coords = feat['geometry']['coordinates'][0][0]
                lat, lon = state_plane_MT_to_WGS(coords[1], coords[0])
                break

        index = date_range(start='20090101', end='20131231', freq='y')
        df = DataFrame(data=None, columns=['name', 'ppt', 'gridmet_etr', 'grimet_eff_ppt', 'agrimet_etr',
                                           'agrimet_eff_ppt', 'Acres_Tot', 'Sq_Meters', 'Acres_Irr', 'Sq_Meters_Irr',
                                           'Weighted_Mean_ET_mm', 'ET_m3', 'ET_af', 'Crop_Cons_m3',
                                           'Crop_Cons_af', 'pivot', 'sprinkler', 'flood'], index=index)

        for yr in YEARS:
            data = [table]
            dt = datetime(int(yr), 12, 31)
            irr_key = 'Irr_{}'.format(yr)
            mean_key = 'mean_{}'.format(yr)
            s, e = datetime.strptime(START.format(yr), FMT), datetime.strptime(END.format(yr), FMT)

            #  gridmet params
            gridmet = GridMet('pr', start=s, end=e, lat=lat, lon=lon)
            ts_ppt = gridmet.get_point_timeseries()
            m_ppt = ts_ppt.groupby(lambda x: x.month).sum().values
            gridmet = GridMet('etr', start=s, end=e, lat=lat, lon=lon)
            ts_etr = gridmet.get_point_timeseries()
            m_etr = ts_etr.groupby(lambda x: x.month).sum().values

            #  agrimet data
            agrimet = Agrimet(station='drlm', start_date=START.format(yr),
                              end_date=END.format(yr), interval='daily')
            formed = agrimet.fetch_data()
            m_agri_etr = formed['ETRS'].groupby(lambda x: x.month).sum().values

            #  effective precipitation calculation
            gridmet_eff_ppt = effective_precip(m_ppt, m_etr)
            m_agrimet_eff_ppt = effective_precip(m_ppt, m_agri_etr)
            season_agri_etr, season_agri_eff_ppt = m_agri_etr.sum(), m_agrimet_eff_ppt.sum()
            season_ppt, season_etr, season_eff_ppt = m_ppt.sum(), m_etr.sum(), gridmet_eff_ppt.sum()
            [data.append(x) for x in [season_ppt, season_etr, season_eff_ppt, season_agri_etr, season_agri_eff_ppt]]

            # area params
            irr_df = csv[csv[irr_key] == 1]
            try:
                acres_tot = csv['Acres'].values.sum()
                acres_irr = irr_df['Acres'].values.sum()
            except KeyError:
                try:
                    acres_tot = csv['ACRES'].values.sum()
                    acres_irr = irr_df['ACRES'].values.sum()
                except KeyError:
                    acres_tot = csv['acres'].values.sum()
                    acres_irr = irr_df['acres'].values.sum()

            sq_m_tot = csv['Sq_Meters'].values.sum()

            try:
                diff = abs(acres_tot - (sq_m_tot / 4046.86))
                assert diff < 1.0
            except AssertionError:
                print('Area check: {} acres should be {} '
                      'sq m, actual is {} sq m'.format(acres_tot,
                                                       acres_tot * 4046.86,
                                                       sq_m_tot))

            sq_m_irr = irr_df['Sq_Meters'].values.sum()
            [data.append(x) for x in [acres_tot, sq_m_tot, acres_irr, sq_m_irr]]

            # irrigation volumes
            mean_mm = (irr_df[mean_key] * irr_df['Sq_Meters'] / irr_df['Sq_Meters'].values.sum()).values.sum()
            et_vol_yr_m3 = (irr_df['Sq_Meters'] * irr_df[mean_key] / 1000.).values.sum()
            et_vol_yr_af = (irr_df['Sq_Meters'] * irr_df[mean_key] / (1000. * 1233.48)).values.sum()
            cc_vol_yr_cm = (irr_df['Sq_Meters'] * (irr_df[mean_key] - season_eff_ppt) / 1000.).values.sum()
            cc_vol_yr_af = (irr_df['Sq_Meters'] * (irr_df[mean_key] - season_eff_ppt) / (1000. * 1233.48)).values.sum()
            [data.append(x) for x in [mean_mm, et_vol_yr_m3, et_vol_yr_af, cc_vol_yr_cm, cc_vol_yr_af]]

            #  irrigation types
            count = irr_df[irr_key].values.sum()
            try:
                p = irr_df.IType.value_counts()['P'] / float(count)
            except KeyError:
                p = 0.0
            except AttributeError:
                p = 'UNK'
            data.append(p)

            try:
                s = irr_df.IType.value_counts()['S'] / float(count)
            except KeyError:
                s = 0.0
            except AttributeError:
                s = 'UNK'
            data.append(s)

            try:
                f = irr_df.IType.value_counts()['F'] / float(count)
            except KeyError:
                f = 0.0
            except AttributeError:
                f = 'UNK'
            data.append(f)
            df.loc[dt] = data

        master = concat([master, df])

    master.to_csv(os.path.join(out_loc, 'OE_Irrigation_Summary_2.csv'), date_format='%Y')


def state_plane_MT_to_WGS(y, x):
    in_proj = Proj(
        '+proj=lcc +lat_1=45 +lat_2=49 +lat_0=44.25 +lon_0=-109.5 +x_0=600000 +y_0=0 '
        '+ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs', preserve_units=True)
    x, y = in_proj(x, y, inverse=True)

    return y, x


if __name__ == '__main__':
    home = os.path.expanduser('~')
    shapefile = os.path.join(home, 'IrrigationGIS', 'OE_Shapefiles')
    table = os.path.join(home, 'IrrigationGIS', 'ssebop_exports')
    existing = os.path.join(table, 'OE_Irrigation_Summary_2.csv')
    # table = os.path.join(home, 'IrrigationGIS', 'ssebop_ancillary')
    build_summary_table(shapefile, table, table)
    # natural_sites_shp(table)

# ========================= EOF ====================================================================
