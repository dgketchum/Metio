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
from pprint import pprint

from numpy import log
from pandas import read_csv, to_datetime
from refet import calcs
from refet.daily import Daily

COLUMNS = ['Timestamp',
           'W/m² Solar Radiation',
           'mm Precipitation',
           ' Lightning Activity',
           'km Lightning Distance',
           '° Wind Direction',
           'm/s Wind Speed',
           'm/s Gust Speed',
           '°C Air Temperature',
           'kPa Vapor Pressure',
           'kPa Atmospheric Pressure',
           '° X-axis Level',
           '° Y-axis Level',
           'mm/h Max Precip Rate',
           '°C RH Sensor Temp',
           'kPa VPD',
           'm³/m³ Water Content',
           '°C Soil Temperature',
           'mS/cm Saturation Extract EC',
           'm³/m³ Water Content.1',
           '°C Soil Temperature.1',
           'mS/cm Saturation Extract EC.1',
           'm³/m³ Water Content.2',
           '°C Soil Temperature.2',
           'mS/cm Saturation Extract EC.2',
           'm³/m³ Water Content.3',
           '°C Soil Temperature.3',
           'mS/cm Saturation Extract EC.3',
           '% Battery Percent',
           'mV Battery Voltage',
           'kPa Reference Pressure',
           '°C Logger Temperature']

RENAME_COLS = {'Solar Radiation': 'W/m²',
               'Precipitation': 'mm',
               'Lightning Activity': '',
               'Lightning Distance': 'km',
               'Wind Direction': 'deg',
               'Wind Speed': 'm/s',
               'Gust Speed': 'm/s',
               'Air Temperature': 'deg C',
               'Vapor Pressure': 'kPa',
               'Atmospheric Pressure': 'kPa',
               'X-axis Level': 'deg',
               'Y-axis Level': 'deg',
               'Max Precip Rate': 'mm/hr',
               'RH Sensor Temp': 'deg C',
               'VPD': 'kPa',
               'Water Content': '',
               'Soil Temperature': 'deg C',
               'Saturation Extract EC': 'mS/cm',
               'Water Content.1': '',
               'Soil Temperature.1': 'deg C',
               'Saturation Extract EC.1': 'mS/cm',
               'Water Content.2': '',
               'Soil Temperature.2': 'deg C',
               'Saturation Extract EC.2': 'mS/cm',
               'Water Content.3': '',
               'Soil Temperature.3': 'deg C',
               'Saturation Extract EC.3': 'mS/cm',
               'Battery Percent': 'percent',
               'Battery Voltage': 'mV',
               'Reference Pressure': 'kPa',
               'Logger Temperature': 'deg C'}


def parse_mesonet(csv):
    csv = read_csv(csv, header=2)
    for x, y in zip(COLUMNS, csv.columns):
        assert x == y, 'Incoming csv doesnt match expected format'.format(
            pprint(COLUMNS, depth=1))

    csv.index = to_datetime(csv['Timestamp'], format='%m/%d/%Y %H:%M')
    csv.drop(columns=['Timestamp'], inplace=True)
    csv.columns = RENAME_COLS.keys()
    csv['etr'] = mesonet_etr(csv, lat=46.3)
    return csv


def mesonet_etr(df, lat=46.3, elevation=1000):
    doy = df.index.strftime('%j').astype(int)
    t_dew = dewpoint_temp(df['Vapor Pressure']).values
    ea = calcs._sat_vapor_pressure(t_dew)
    tmin, tmax = df['MN'].values, df['MX'].values
    doy = doy.reshape((len(doy), 1))[0]
    rs = df['SR'].values
    uz = df['UA'].values
    zw = 2.0
    df['calc_etr'] = Hourly(tmin=tmin, tmax=tmax, ea=ea, rs=rs, uz=uz, zw=zw, elev=elevation,
                            lat=lat, doy=doy).etr()
    calc_etr = formed['calc_etr'].groupby(lambda x: x.month).sum().values
    return calc_etr


def dewpoint_temp(e):
    dew_temp = (log(e) + 0.4926) / (0.0708 - 0.00421 * log(e))
    return dew_temp


if __name__ == '__main__':
    home = os.path.expanduser('~')
    mes_dir = os.path.join(home, 'IrrigationGIS', 'lolo', 'mesonet')
    _file = os.path.join(mes_dir, 'MBMG LL 148(06-00148).csv')
    parse_mesonet(_file)
# ========================= EOF ====================================================================
