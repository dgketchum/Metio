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


def suite():

    print('Testing.......................................')

    from tests.test_gridmet import TestGridMet
    from tests.test_agrimet import TestAgrimet
    from tests.test_eddy_flux import EddyTowerTestCase
    from tests.test_topowx import TestTopoWX

    loader = unittest.TestLoader()
    test_suite = unittest.TestSuite()

    tests = (TestGridMet, TestAgrimet, EddyTowerTestCase, TestTopoWX,)

    for t in tests:
        test_suite.addTest(loader.loadTestsFromTestCase(t))

    return test_suite


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())

# ===============================================================================
