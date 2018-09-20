import unittest2 as unittest
from tinctest.lib import local_path
from mpp.lib.PSQL import PSQL
from mpp.lib.gpdbverify import GpdbVerify

class GpdbVerifyRegressionTests(unittest.TestCase):

    def __init__(self, methodName):
        self.gpv = GpdbVerify()
        super(GpdbVerifyRegressionTests, self).__init__(methodName)
    
    def setUp(self):
        PSQL.run_sql_command('create database gptest;', dbname='postgres')

    def tearDown(self):
        PSQL.run_sql_command('drop database gptest', dbname='postgres')

    def test_gpcheckcat(self):
        (a,b,c,d) = self.gpv.gpcheckcat()
        self.assertIn(a,(0,1,2))

    def test_gpcheckmirrorseg(self):
        (res,fix_file) = self.gpv.gpcheckmirrorseg()
        self.assertIn(res, (True,False))

    def test_check_db_is_running(self):
        self.assertTrue(self.gpv.check_db_is_running())

    def test_run_repairscript(self):
        repair_script = local_path('gpcheckcat_repair')
        res = self.gpv.run_repair_script(repair_script)
        self.assertIn(res, (True,False))

    def test_ignore_extra_m(self):
        fix_file = local_path('fix_file')
        res = self.gpv.ignore_extra_m(fix_file)
        self.assertIn(res, (True,False))
