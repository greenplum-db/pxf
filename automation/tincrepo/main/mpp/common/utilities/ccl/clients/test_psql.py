#!/usr/bin python

import os
import commands
import tinctest

class CCLPsqlTestCase(tinctest.TINCTestCase):
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

    def tearDown(self):
        pass

    def run_psql_file(self, sqlfile, ofile = None):
        if ofile:
            cmd = 'psql -h %s -p %s -U %s -d %s -f %s > %s 2>&1' % (self.pghost, self.pgport, self.pguser, self.pgdb, sqlfile, ofile)
        else:
            cmd = 'psql -h %s -p %s -U %s -d %s -f %s' % (self.pghost, self.pgport, self.pguser, self.pgdb, sqlfile)
        (ret, output) = commands.getstatusoutput(cmd)
        return ret

    def test_psql_simple(self):
        sqlfile = self._get_absolute_filename('test_psql_simple.sql')
        ofile = self._get_absolute_filename('test_psql_simple.out')
        self.assertEqual(0, self.run_psql_file(sqlfile, ofile))
