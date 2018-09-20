"""
Contains tests for testing scenarios within other scenarios
"""

from datetime import datetime
import fnmatch
import inspect
import os
from time import sleep

from contextlib import closing
from StringIO import StringIO
from unittest2.runner import _WritelnDecorator

import unittest2 as unittest
from unittest2.result import TestResult

import tinctest
from tinctest.case import TINCTestCase
from tinctest.models.scenario import ScenarioTestCase
from tinctest.models.concurrency import ConcurrencyTestCase
from tinctest.runner import TINCTextTestResult, TINCTestRunner


@unittest.skip('mock')
class MockGrandParentScenarioTestCase(ScenarioTestCase):
    def test_scenario_recursive_containment(self):
        self.test_case_scenario.append(['tinctest.models.scenario.test.test_scenario_containment.MockParentScenarioTestCase.test_scenario_containing_another_scenario'])

@unittest.skip('mock')
class MockParentScenarioTestCase(ScenarioTestCase):    
    def test_scenario_containing_another_scenario(self):
        self.test_case_scenario.append(['tinctest.models.scenario.test.test_scenario_containment.MockContainedScenarioTestCase'])

    def test_scenario_containment_with_other_tests(self):
        """
        Add a scenario test to another scenario along with other tests
        """
        self.test_case_scenario.append(['tinctest.models.scenario.test.test_scenario_containment.MockContainedScenarioTestCase',
                                        'tinctest.models.scenario.test.test_scenario_containment.MockTestCase3'])
        self.test_case_scenario.append(['tinctest.models.scenario.test.test_scenario_containment.MockTestCase1'])
        

@unittest.skip('mock')
class MockContainedScenarioTestCase(ScenarioTestCase):
    def test_contained_scenario_method(self):
        self.test_case_scenario.append(['tinctest.models.scenario.test.test_scenario_containment.MockTestCase1.test_01',
                                        'tinctest.models.scenario.test.test_scenario_containment.MockTestCase1.test_02'])
        self.test_case_scenario.append(['tinctest.models.scenario.test.test_scenario_containment.MockTestCase2.test_01',
                                        'tinctest.models.scenario.test.test_scenario_containment.MockTestCase2.test_02'])
        
@unittest.skip('mock')
class MockTestCase1(TINCTestCase):
        
    def test_01(self):
        pass

    def test_02(self):
        pass        

@unittest.skip('mock')
class MockTestCase2(TINCTestCase):
    def test_01(self):
        self.tested = True
        self.assertTrue(self.tested)
        
    def test_02(self):
        self.tested = True
        self.assertTrue(self.tested)

@unittest.skip('mock')
class MockTestCase3(TINCTestCase):
    def test_01(self):
        pass

    def test_02(self):
        pass


class ScenarioContainmentTestCases(unittest.TestCase):

    def test_scenario_within_scenario(self):
        """
        MockParentScenarioTestCase.test_scenario_containing_another_scenario adds a scenario test case within
        it's scenario. The added scenario contains two additional steps and this tests whether those steps are added
        to the current scenario and executed.
        """
        tinc_test_case = MockParentScenarioTestCase('test_scenario_containing_another_scenario')
        tinc_test_case.__class__.__unittest_skip__ = False
        results = TestResult()
        tinc_test_case.run(results)
        # This should have added the two steps from the contained scenario test case
        self.assertEquals(3, len(tinc_test_case._scenario.steps))
        self.assertEquals(results.testsRun, 1)
        self.assertEquals(len(results.failures), 0)
        self.assertEquals(len(results.errors), 0)
        self.assertEquals(len(results.skipped), 0)

        # Verify the steps
        step0 = tinc_test_case._scenario.steps[0]
        step1 = tinc_test_case._scenario.steps[1]
        step2 = tinc_test_case._scenario.steps[2]

        # The first step should not contain any tests as it contains only the scenario test case
        self.assertEquals(len(step0.tests), 0)
        # step1 and step2 should contain two tests each
        self.assertEquals(len(step1.tests), 2)
        self.assertEquals(len(step2.tests), 2)

    def test_scenario_containment_with_other_tests(self):
        """
        MockParentScenarioTestCase.test_scenario_containment_with_other_tests adds a scenario test case within
        it's scenario and also other normal tests. The added scenario contains two additional steps and this tests whether those steps are added
        to the current scenario and executed.
        """
        tinc_test_case = MockParentScenarioTestCase('test_scenario_containment_with_other_tests')
        tinc_test_case.__class__.__unittest_skip__ = False
        results = TestResult()
        tinc_test_case.run(results)

        # This should have added the two steps from the contained scenario test case to the steps in the parent scenario
        self.assertEquals(4, len(tinc_test_case._scenario.steps))
        self.assertEquals(results.testsRun, 1)
        self.assertEquals(len(results.failures), 0)
        self.assertEquals(len(results.errors), 0)
        self.assertEquals(len(results.skipped), 0)

        # Verify the steps
        step0 = tinc_test_case._scenario.steps[0]
        step1 = tinc_test_case._scenario.steps[1]
        step2 = tinc_test_case._scenario.steps[2]
        step3 = tinc_test_case._scenario.steps[3]

        # The first step should not contain two tests from MockTestCase3
        self.assertEquals(len(step0.tests), 2)
        for test in step0.tests:
            self.assertTrue(test == 'MockTestCase3.test_01' or test == 'MockTestCase3.test_02')
        # step1 and step2 should contain two tests each - steps added from the contained scenario
        self.assertEquals(len(step1.tests), 2)
        for test in step1.tests:
            self.assertTrue(test == 'MockTestCase1.test_01' or test == 'MockTestCase1.test_02')
        
        self.assertEquals(len(step2.tests), 2)
        for test in step2.tests:
            self.assertTrue(test == 'MockTestCase2.test_01' or test == 'MockTestCase2.test_02')

        # Step3 should be the from the parent scenario test case
        self.assertEquals(len(step3.tests), 2)
        for test in step3.tests:
            self.assertTrue(test == 'MockTestCase1.test_01' or test == 'MockTestCase1.test_02')

    def test_scenario_recursive_containment(self):
        """
        MockParentScenarioTestCase.test_scenario_containment_with_other_tests adds a scenario test case within
        it's scenario and also other normal tests. The added scenario contains two additional steps and this tests whether those steps are added
        to the current scenario and executed.
        """
        tinc_test_case = MockGrandParentScenarioTestCase('test_scenario_recursive_containment')
        tinc_test_case.__class__.__unittest_skip__ = False
        results = TestResult()
        tinc_test_case.run(results)

        # This should have added the two steps from the contained scenario test case, one empty step in the grandparent scenario ,
        # one empty step in the parent scenario
        self.assertEquals(4, len(tinc_test_case._scenario.steps))
        self.assertEquals(results.testsRun, 1)
        self.assertEquals(len(results.failures), 0)
        self.assertEquals(len(results.errors), 0)
        self.assertEquals(len(results.skipped), 0)

        # Verify the steps
        step0 = tinc_test_case._scenario.steps[0]
        step1 = tinc_test_case._scenario.steps[1]
        step2 = tinc_test_case._scenario.steps[2]
        step3 = tinc_test_case._scenario.steps[3]

        # The first step and second step should be empty
        self.assertTrue(len(step0.tests) == 0)
        self.assertTrue(len(step1.tests) == 0)

        # step2 should contain tests from MockTestCase1 added by the contained scenario test case
        self.assertTrue(len(step2.tests) == 2)
        for test in step2.tests:
            self.assertTrue(test == 'MockTestCase1.test_01' or test == 'MockTestCase1.test_02')

        # step3 should contain tests from MockTestCase2 added by the contained scenario test case
        self.assertTrue(len(step3.tests) == 2)
        for test in step3.tests:
            self.assertTrue(test == 'MockTestCase2.test_01' or test == 'MockTestCase2.test_02')

        
        

        


    
        
        
