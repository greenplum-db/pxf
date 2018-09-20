#!/usr/bin python

import os
import commands
import tinctest

class CCLGpmapreduceTestCase(tinctest.TINCTestCase):
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        self.pghost = os.getenv('PGHOST', '127.0.0.1')
        self.pgport = os.getenv('PGPORT', '5432')
        self.pguser = os.getenv('PGUSER', 'gpadmin')
        self.pgdb = os.getenv('PGDATABASE', 'postgres')
        self.run_psql_file(self._get_absolute_filename('test_gpmapreduce_setup.sql'))

    def tearDown(self):
        self.run_psql_file(self._get_absolute_filename('test_gpmapreduce_teardown.sql'))

    def run_psql_file(self, sqlfile, ofile = None):
        if ofile:
            cmd = 'psql -h %s -p %s -U %s -d %s -f %s > %s 2>&1' % (self.pghost, self.pgport, self.pguser, self.pgdb, sqlfile, ofile)
        else:
            cmd = 'psql -h %s -p %s -U %s -d %s -f %s' % (self.pghost, self.pgport, self.pguser, self.pgdb, sqlfile)
        (ret, output) = commands.getstatusoutput(cmd)
        return ret

    def test_gpmapreduce_simple(self):
        ymlfile = self._get_absolute_filename('test_gpmapreduce_simple.yml')
        ofile = self._get_absolute_filename('test_gpmapreduce_simple.out')
        self.assertEqual(0, self.run_gpmr_file(ymlfile, ofile))

    def run_gpmr_file(self, ymlfile, ofile):
        cmd = 'gpmapreduce -h %s -p %s -U %s -f %s %s > %s 2>&1' % (self.pghost, self.pgport, self.pguser, ymlfile, self.pgdb, ofile)
        (ret, output) = commands.getstatusoutput(cmd)
        return ret

