import unittest2 as unittest
from mpp.lib.PSQL import PSQL
from mpp.lib.gpfilespace import Gpfilespace
from mpp.lib.gpfilespace import GPDBGpfilespace

class GpFilespaceRegressionTests(unittest.TestCase):

    def __init__(self, methodName):
        self.gpfs = Gpfilespace()
        self.gpfs_h = GPDBGpfilespace()
        super(GpFilespaceRegressionTests, self).__init__(methodName)

    def tearDown(self):
        PSQL.run_sql_command('Drop filespace test_fs_a;')

    def test_create_filespace(self):
        self.gpfs.create_filespace('test_fs_a')
        fs_list = PSQL.run_sql_command("select fsname from pg_filespace where fsname<>'pg_system';", flags = '-q -t')
        self.assertTrue('test_fs_a' in fs_list)

    def test_drop_fiespace(self):
        self.gpfs.create_filespace('test_fs_b')
        self.assertTrue(self.gpfs.drop_filespace('test_fs_b'))

    def test_fs_exists(self):
        self.gpfs.create_filespace('test_fs_a')
        self.assertTrue(self.gpfs.exists('test_fs_a'))

    def test_showtempfiles(self):
        result = self.gpfs.showtempfiles()
        show = False
        for line in result.stdout.splitlines():
            if 'Current Filespace for TEMPORARY_FILES' in line:
                show = True
        self.assertTrue(show)
    
    def test_get_filespace_location(self):
        result = self.gpfs.get_filespace_location()
        self.assertTrue(len(result) >0)
    
    def test_get_filespace_directory(self):
        result = self.gpfs.get_filespace_directory()
        self.assertTrue(len(result) >0)

    def test_get_hosts_for_filespace(self):
        self.gpfs.create_filespace('test_fs_a')
        fs_location = PSQL.run_sql_command("select fselocation  from pg_filespace_entry where fselocation like '%test_fs_a%' and fsedbid=2;", flags = '-q -t')
        result = self.gpfs.get_hosts_for_filespace(fs_location.strip())
        self.assertEquals(result[0]['location'],fs_location.strip())
        
    def test_create_filespace_gpdb(self):
        self.gpfs_h.create_filespace('test_fs_hq')
        fs_list = PSQL.run_sql_command("select fsname from pg_filespace where fsname<>'pg_system';", flags = '-q -t')
        self.assertTrue('test_fs_hq' in fs_list)
        #self.gpfs.drop_filespace('test_fs_hq')

