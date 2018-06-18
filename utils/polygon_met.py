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
from numpy import vstack, hstack
from pandas import read_csv, DataFrame, date_range, concat
from fiona import open as fopen
from pyproj import Proj
from datetime import datetime

from met.thredds import GridMet

TABLES = ['Broadwater_Missouri_Canal',
          'Broadwater_Missouri_West_Side_Canal',
          'Dodson_North_Canal_Diversion',
          'Dodson_South_Div_To_Bowdoin',
          'East_Fork_Main_Canal_ab_Trout_Creek',
          'Eldorado',
          'Floweree_and_Floweree_Hamilton',
          'Fort_Belknap_Main_Diversion',
          'Fort_Shaw_Canal',
          'Glasgow_ID',
          'Huntley_Main_Diversion',
          'LYIP_North_Dakota',
          'Marshall_Canal',
          'Paradise_Valley_ID',
          'Ruby_River',
          'Sun_River_project_Below_Pishkun',
          'Vigilante_Canal',
          'West_Bench_Canal',
          'Yellowstone_Main_Diversion']

I_TYPES = ['P', 'S', 'F']
YEARS = ['2009', '2010', '2011', '2012', '2013']

START, END = '{}-04-15', '{}-10-15'
FMT = '%Y-%m-%d'

D = 3.0


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


def get_polygon_met_parameters(shapes, tables, out_loc):

    master = DataFrame()

    for table in TABLES:
        print(table)
        csv = read_csv(os.path.join(tables, '{}.csv'.format(table)))

        shp = os.path.join(shapes, '{}.shp'.format(table))
        with fopen(shp) as src:
            for feat in src:
                coords = feat['geometry']['coordinates'][0][0]
                lat, lon = state_plane_MT_to_WGS(coords[1], coords[0])
                break

        index = date_range(start='20090101', end='20131231', freq='y')
        df = DataFrame(data=None, columns=['name', 'ppt', 'eff_ppt', 'etr', 'Acres_Tot', 'Sq_Meters',
                                           'Acres_Irr', 'Sq_Meters_Irr', 'Weighted_Mean_ET_mm',
                                           'ET_m3', 'ET_af', 'Crop_Cons_m3',
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

            #  effective precipitation calculation
            eff_ppt = effective_precip(m_ppt, m_etr)
            season_ppt, season_etr, season_eff_ppt = m_ppt.sum(), m_etr.sum(), eff_ppt.sum()
            [data.append(x) for x in [season_ppt, season_eff_ppt, season_etr]]

            # area params
            acres_tot = csv['Acres'].values.sum()
            sq_m_tot = csv['Sq_Meters'].values.sum()
            irr_df = csv[csv[irr_key] == 1]
            acres_irr = irr_df['Acres'].values.sum()
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
            data.append(p)
            try:
                s = irr_df.IType.value_counts()['S'] / float(count)
            except KeyError:
                s = 0.0
            data.append(s)
            try:
                f = irr_df.IType.value_counts()['F'] / float(count)
            except KeyError:
                f = 0.0
            data.append(f)
            df.loc[dt] = data

        master = concat([master, df])


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
    out_loc = os.path.join(home, 'IrrigationGIS', 'ssebop_summaries')
    get_polygon_met_parameters(shapefile, table, out_loc)

# ========================= EOF ====================================================================
