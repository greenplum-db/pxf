#!/usr/bin python

import os
import commands
import re
import fileinput
import subprocess
import tinctest

MYD = os.path.abspath(os.path.dirname(__file__))

class CCLLibpqTestCase(tinctest.TINCTestCase):
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
        
        self.run_psql_file(self._get_absolute_filename('test_libpq_setup.sql'))
        self.check_libpq_env()

    def tearDown(self):
        self.run_psql_file(self._get_absolute_filename('test_libpq_teardown.sql'))

    def run_psql_file(self, sqlfile, ofile = None):
        if ofile:
            cmd = 'psql -h %s -p %s -U %s -d %s -f %s > %s 2>&1' % (self.pghost, self.pgport, self.pguser, self.pgdb, sqlfile, ofile)
        else:
            cmd = 'psql -h %s -p %s -U %s -d %s -f %s' % (self.pghost, self.pgport, self.pguser, self.pgdb, sqlfile)
        (ret, output) = commands.getstatusoutput(cmd)
        return ret

    def check_libpq_env(self):
        if not os.environ['GPHOME_CONNECTIVITY']:
            return False
        return True

    def compileGCC(self, filename):
        cur_path = os.getcwd()
        os.chdir(MYD)
        if os.environ.has_key('GCCFLAGS'):
            cmd = 'gcc %s -o %s %s.c LibpqCommon.c -I%s -L%s -lpq' % (os.environ['GCCFLAGS'], filename, filename, os.environ['GPHOME_CONNECTIVITY'] + os.sep + 'include', os.environ['GPHOME_CONNECTIVITY'] + os.sep + 'lib')
        else:
            cmd = 'gcc -o %s %s.c LibpqCommon.c -I%s -L%s -lpq' % (filename, filename, os.environ['GPHOME_CONNECTIVITY'] + os.sep + 'include', os.environ['GPHOME_CONNECTIVITY'] + os.sep + 'lib')
        (ret, output) = commands.getstatusoutput(cmd)
        os.chdir(cur_path)
        return ret

    def doTest(self, filename):
        cur_path = os.getcwd()
        os.chdir(MYD)
        self.compileGCC(filename)
        cmd = './%s > %s.out 2>&1' % (filename, filename)
        (ret, output) = commands.getstatusoutput(cmd)
        os.chdir(cur_path)
        if ret != 0:
            return False
        return True

    def test_connect_success(self):
        self.assertTrue(self.doTest('TestLibpqConnSuccess'))

    def test_connect_fail(self):
        self.assertTrue(self.doTest('TestLibpqConnFail'))

    def test_create_and_drop_table(self):
        self.assertTrue(self.doTest('TestLibpqCreateAndDropTable'))
    
    def test_insert_sql(self):
        self.assertTrue(self.doTest('TestLibpqInsertSQL'))

    def test_select_sql(self):
        self.assertTrue(self.doTest('TestLibpqSelectSQL'))

    def test_transaction_commit(self):
        self.assertTrue(self.doTest('TestLibpqTransactionCommit'))

    def test_transaction_rollback(self):
        self.assertTrue(self.doTest('TestLibpqTransactionRollback'))

    def test_transaction_savepoint(self):
        self.assertTrue(self.doTest('TestLibpqTransactionSavePoint'))


