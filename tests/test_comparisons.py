import unittest
from datetime import datetime

from numpy import nanmean
from matplotlib import pyplot as plt

from met.agrimet import Agrimet
from met.thredds import GridMet
from met.mesonet import parse_mesonet


class MyTestCase(unittest.TestCase):

    def setUp(self):
        self.fetch_site = 'covm'
        self.start = '2015-01-01'
        self.end = '2015-12-31'
        self.lat = 46.34
        self.lon = -112.77
        self.covm_mco = 'ARC-W Corvallis(06-00151).csv'

    def test_agrimet_gridmet_precip(self):
        agrimet = Agrimet(station=self.fetch_site, start_date=self.start,
                          end_date=self.end, interval='daily')

        formed = agrimet.fetch_met_data()
        agri_ppt = formed['PP'].values

        s, e = datetime.strptime(self.start, '%Y-%m-%d'), \
               datetime.strptime(self.end, '%Y-%m-%d')
        gridmet = GridMet('pr', start=s, end=e,
                          lat=self.lat, lon=self.lon)
        gridmet_ppt = gridmet.get_point_timeseries()
        gridmet_ppt = gridmet_ppt.values

        difference = abs(gridmet_ppt - agri_ppt)
        self.assertLess(nanmean(difference), -100000)

    def test_agrimet_gridmet_etr(self):
        agrimet = Agrimet(station=self.fetch_site, start_date=self.start,
                          end_date=self.end, interval='daily')

        formed = agrimet.fetch_met_data()
        agri_etr = formed['ETRS'].values

        s, e = datetime.strptime(self.start, '%Y-%m-%d'), \
               datetime.strptime(self.end, '%Y-%m-%d')
        gridmet = GridMet('etr', start=s, end=e,
                          lat=self.lat, lon=self.lon)
        gridmet_etr = gridmet.get_point_timeseries()
        gridmet_etr = gridmet_etr.values

        plt.plot(gridmet_etr, label='gridmet')
        plt.plot(agri_etr, label='agrimet')
        plt.legend()
        plt.show()
        ratio = agri_etr.sum() / gridmet_etr.sum()
        print('ratio: {}'.format(ratio))

    def test_agrimet_mesonet_gridmet_etr(self):
        agrimet = Agrimet(station=self.fetch_site, start_date=self.start,
                          end_date=self.end, interval='daily')

        formed = agrimet.fetch_met_data()
        agri_etr = formed['ETRS'].values

        s, e = datetime.strptime(self.start, '%Y-%m-%d'), \
               datetime.strptime(self.end, '%Y-%m-%d')
        gridmet = GridMet('etr', start=s, end=e,
                          lat=self.lat, lon=self.lon)
        gridmet_etr = gridmet.get_point_timeseries()
        gridmet_etr = gridmet_etr.values

        plt.plot(gridmet_etr, label='gridmet')
        plt.plot(agri_etr, label='agrimet')
        plt.legend()
        plt.show()
        ratio = gridmet_etr.sum() / agri_etr.sum()
        print('ratio: {}'.format(ratio))


if __name__ == '__main__':
    unittest.main()
# ========================= EOF ====================================================================
