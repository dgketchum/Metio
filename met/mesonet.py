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
from pandas import read_csv, to_datetime, DataFrame, date_range
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


class Mesonet(object):

    def __init__(self, csv, start=None, end=None):

        self.csv = csv
        self.start = start
        self.end = end
        self.table = None
        self.df = None
        self._parse_csv()

    def _parse_csv(self):

        self.table = read_csv(self.csv, header=2)
        for x, y in zip(COLUMNS, self.table.columns):
            assert x == y, 'Incoming csv doesnt match expected format'.format(
                pprint(COLUMNS, depth=1))

        self.table.index = to_datetime(self.table['Timestamp'], format='%m/%d/%Y %H:%M')
        self.table.drop(columns=['Timestamp'], inplace=True)
        self.table.columns = RENAME_COLS.keys()

        if self.start and self.end:
            self.table = self.table.ix[self.start: self.end]

    def mesonet_ppt(self, daily=True):
        if daily:
            ppt_daily = self.table['Precipitation'].resample('D').sum().values
            df = DataFrame(ppt_daily, columns=['Precipitation'])
            df.index = date_range(self.table.index.min(), self.table.index.max(), freq='D')
            return df
        else:
            return DataFrame(self.table['Precipitation'].values, columns=['Precipitation'])

    def mesonet_etr(self, lat=46.3, elevation=1000):
        t_dew = dewpoint_temp(self.table['Vapor Pressure'].resample('D').mean().values)
        ea = calcs._sat_vapor_pressure(t_dew)
        tmax_series = self.table['Air Temperature']
        tmax = tmax_series.resample('D').max().values
        tmin = self.table['Air Temperature'].resample('D').min().values
        daily = tmax_series.resample('D').mean()
        doy = daily.index.strftime('%j').astype(int).values
        rs = self.table['Solar Radiation'].resample('D').mean().values * 0.0864
        uz = self.table['Wind Speed'].resample('D').mean().values
        zw = 2.4
        etr = Daily(tmin=tmin, tmax=tmax, ea=ea, rs=rs, uz=uz, zw=zw, elev=elevation,
                    lat=lat, doy=doy).etr()
        df = DataFrame(data=[doy, rs, etr, uz, ea, t_dew, tmax, tmin]).transpose()
        df.columns = ['DOY', 'SR', 'ETR', 'UZ', 'EA', 'TDew', 'TMax', 'TMin']
        df.index = date_range(self.table.index.min(), self.table.index.max(), freq='D')
        return df


def dewpoint_temp(e):
    dew_temp = (log(e) + 0.4926) / (0.0708 - 0.00421 * log(e))
    return dew_temp


if __name__ == '__main__':
    home = os.path.expanduser('~')
# ========================= EOF ====================================================================
