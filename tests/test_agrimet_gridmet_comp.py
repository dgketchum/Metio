import unittest
from met.agrimet import Agrimet
from met.thredds import GridMet
from datetime import datetime
from numpy import nanmean


class MyTestCase(unittest.TestCase):

    def setUp(self):
        self.site_ids = ['robi', 'hntu', 'mdxo', 'mdso', 'masw']
        self.start = '2015-01-01'
        self.end = '2015-12-31'
        self.lat = 46.34
        self.lon = -112.77

    def test_agrimet_gridmet(self):

        for site in self.site_ids:

            agrimet = Agrimet(station=site, start_date=self.start,
                              end_date=self.end, interval='daily')

            formed = agrimet.fetch_data()
            agri_tmax = formed['MX'].values

            s, e = datetime.strptime(self.start, '%Y-%m-%d'), \
                   datetime.strptime(self.end, '%Y-%m-%d')
            gridmet = GridMet('tmmx', start=s, end=e,
                              lat=self.lat, lon=self.lon)
            gridmet_tmax = gridmet.get_point_timeseries()
            gridmet_tmax = (gridmet_tmax - 273.15).values
            difference = (gridmet_tmax - agri_tmax) / agri_tmax
            print(site)
            self.assertLess(nanmean(difference), 0.1)


if __name__ == '__main__':
    unittest.main()
# ========================= EOF ====================================================================
