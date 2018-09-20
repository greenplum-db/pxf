import unittest2 as unittest

import tinctest

from tinctest import TINCTestCase
from tinctest.models.scenario import ScenarioTestCase

class ScenarioTestCaseWithDataProvider(ScenarioTestCase):
    """
    @author kodunh
    @maintainer kodunh
    @description a very basic test case that constructs a scenario test case
                 the use case is to test QAINF-769 to make sure that the test case
                 does not run the scenario multipl time for each data provider type
    """
    def test_sample(self):
        """
        @data_provider test1
        """
        test_case_list0 = []
        test_case_list0.append('tinctest.models.scenario.test.test_scenario_data_provider.MockTC.test_step1_01')
        test_case_list0.append('tinctest.models.scenario.test.test_scenario_data_provider.MockTC.test_step1_02')
        test_case_list0.append('tinctest.models.scenario.test.test_scenario_data_provider.MockTC.test_step1_03')
        self.test_case_scenario.append(test_case_list0)
        self.assertEquals(len(self.test_case_scenario), 1)


@unittest.skip('Mock')
class MockTC(TINCTestCase):
    '''
    Right now empty function because nothing is necessary in these functions
    to test QAINF-769. If/When a need arises to update this test case and please
    add appropriate code to each function and update this description as well
    '''
    def test_step1_01(self):
        pass

    def test_step1_02(self):
        pass

    def test_step1_03(self):
        pass

@tinctest.dataProvider('test1')
def test_data_provider():
    data = {'type1': ['int', 'int2'], 'type2': ['varchar']}
    return data
