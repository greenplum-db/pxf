#!/usr/bin python

import os
import commands
import re
import subprocess
import fileinput
import tinctest
import string

MYD = os.path.abspath(os.path.dirname(__file__))

class CCLJdbcTestCase(tinctest.TINCTestCase):
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

        self.run_psql_file(self._get_absolute_filename('test_jdbc_setup.sql'))
        self.compileJava("ConnectionManager")
        self.compileJava("JDBCCommon")
        self.check_jdbc_driver()

    def tearDown(self):
        self.run_psql_file(self._get_absolute_filename('test_jdbc_teardown.sql'))

    def run_psql_file(self, sqlfile, ofile = None):
        if ofile:
            cmd = 'psql -h %s -p %s -U %s -d %s -f %s > %s 2>&1' % (self.pghost, self.pgport, self.pguser, self.pgdb, sqlfile, ofile)
        else:
            cmd = 'psql -h %s -p %s -U %s -d %s -f %s' % (self.pghost, self.pgport, self.pguser, self.pgdb, sqlfile)
        (ret, output) = commands.getstatusoutput(cmd)
        return ret

    def check_jdbc_driver(self):
        if not os.environ['GPHOME_CONNECTIVITY']:
            return False
        driver_base_dir = os.environ['GPHOME_CONNECTIVITY'] + os.sep + 'drivers' + os.sep + 'jdbc'
        self.drivers = os.listdir(driver_base_dir)
        self.connectivity_config_file = os.environ['GPHOME_CONNECTIVITY'] + os.sep + 'greenplum_connectivity_path.sh'
        if len(self.drivers) <= 0:
            return False
        return True

    def config_jdbc_driver(self, drivername):
        driver_key = 'GP_JDBC_JARFILE'
        p = re.compile('^%s' % driver_key)
        for line in fileinput.input(self.connectivity_config_file, inplace=1):
            if p.match(line):
                print driver_key + '=' + drivername
            else:
                print line,
        command = ['bash', '-c', 'source %s && env' % (self.connectivity_config_file)]
        proc = subprocess.Popen(command, stdout = subprocess.PIPE)
        for line in proc.stdout:
            if line[-1] == '\n':
                line = line[0:-1]
            if len(string.split(line, '=')) != 2:
                continue
            res = string.split(line, '=')
            if res[0] and res[0] == 'CLASSPATH':
                os.environ['CLASSPATH'] = res[1]
        proc.communicate()

    def compileJava(self, filename):
        if not os.path.exists(self._get_absolute_filename(filename+".class")):
            cur_path = os.getcwd()
            os.chdir(MYD)
            cmd = 'javac %s.java' % (filename)
            (ret, output) = commands.getstatusoutput(cmd)
            os.chdir(cur_path)
            return ret
        else:
            return 0

    def doTest(self, filename):
        for jdbc_driver in self.drivers:
            self.config_jdbc_driver(jdbc_driver)
            cur_path = os.getcwd()
            os.chdir(MYD)
            self.compileJava(filename)
            cmd = 'java %s > %s.out 2>&1' % (filename, filename)
            (ret, output) = commands.getstatusoutput(cmd)
            os.chdir(cur_path)
            if ret != 0:
                return False
        return True

    def test_connect_success(self):
        self.assertTrue(self.doTest('TestJDBCConnSuccess'))

    def test_connect_fail(self):
        self.assertTrue(self.doTest('TestJDBCConnFail'))

    def test_create_and_drop_table(self):
        self.assertTrue(self.doTest('TestJDBCCreateAndDropTable'))
    
    def test_insert_sql(self):
        self.assertTrue(self.doTest('TestJDBCInsertSQL'))

    def test_select_sql(self):
        self.assertTrue(self.doTest('TestJDBCSelectSQL'))

    def test_transaction_commit(self):
        self.assertTrue(self.doTest('TestJDBCTransactionCommit'))

    def test_transaction_rollback(self):
        self.assertTrue(self.doTest('TestJDBCTransactionRollback'))

