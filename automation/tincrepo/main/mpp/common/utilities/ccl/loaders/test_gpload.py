#!/usr/bin python

import os
import commands
import tinctest

class CCLGploadTestCase(tinctest.TINCTestCase):
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
        self.run_psql_file(self._get_absolute_filename('test_gpload_setup.sql'))

    def tearDown(self):
        self.run_psql_file(self._get_absolute_filename('test_gpload_teardown.sql'))

    def run_psql_file(self, sqlfile, ofile = None):
        if ofile:
            cmd = 'psql -h %s -p %s -U %s -d %s -f %s > %s 2>&1' % (self.pghost, self.pgport, self.pguser, self.pgdb, sqlfile, ofile)
        else:
            cmd = 'psql -h %s -p %s -U %s -d %s -f %s' % (self.pghost, self.pgport, self.pguser, self.pgdb, sqlfile)
        (ret, output) = commands.getstatusoutput(cmd)
        return ret

    def test_gpload_simple(self):
        ymlfile = self._get_absolute_filename('test_gpload_simple.yml')
        ofile = self._get_absolute_filename('test_gpload_simple.out')
        hostname = self.update_yml(ymlfile)
        ret = self.run_gpload_file(ymlfile, ofile)
        # ret may be not equal zero because some gpload hostname can not be access by DB server.
        # use below workaround
        if (ret != 0 and len(hostname) <= len('greenplum.com')):
            # full hostname is valid
            print '[WARNING] hostname is not full name : %s' % (hostname)
            return
        else:
            self.assertEqual(0, ret)

    def run_gpload_file(self, ymlfile, ofile):
        MYD = os.path.abspath(os.path.dirname(__file__))
        cur_path = os.getcwd()
        os.chdir(MYD)
        cmd = 'gpload -h %s -p %s -U %s -d %s -f %s > %s 2>&1' % (self.pghost, self.pgport, self.pguser, self.pgdb, ymlfile, ofile)
        (ret, output) = commands.getstatusoutput(cmd)
        os.chdir(cur_path)
        return ret

    def update_yml(self, ymlfile):
        (ret, output) = commands.getstatusoutput('hostname -f')
        if ret != 0:
            (ret, output) = commands.getstatusoutput('hostname')
        hostname = output
        cmd = 'sed -i "9a \        LOCAL_HOSTNAME:" %s' % (ymlfile)
        commands.getstatusoutput(cmd)
        cmd = 'sed -i "10a \          - %s" %s' % (output, ymlfile)
        commands.getstatusoutput(cmd)
        return hostname 


