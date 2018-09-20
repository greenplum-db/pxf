#!/usr/bin python

import os
import commands
import re
import fileinput
import subprocess
import tinctest
import string
import platform

MYD = os.path.abspath(os.path.dirname(__file__))

class CCLOdbcTestCase(tinctest.TINCTestCase):
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
        
        self.drivers = [] 
        self.run_psql_file(self._get_absolute_filename('test_odbc_setup.sql'))
        self.check_odbc_driver()

    def tearDown(self):
        self.run_psql_file(self._get_absolute_filename('test_odbc_teardown.sql'))

    def run_psql_file(self, sqlfile, ofile = None):
        if ofile:
            cmd = 'psql -h %s -p %s -U %s -d %s -f %s > %s 2>&1' % (self.pghost, self.pgport, self.pguser, self.pgdb, sqlfile, ofile)
        else:
            cmd = 'psql -h %s -p %s -U %s -d %s -f %s' % (self.pghost, self.pgport, self.pguser, self.pgdb, sqlfile)
        (ret, output) = commands.getstatusoutput(cmd)
        return ret

    def check_odbc_driver(self):
        """ 
        valid_drivers = {
                'psqlodbc-09.00.0200' : 'unixodbc-2.2.12',
                #'psqlodbc-08.04.0200' : 'unixodbc-2.2.12',
                #'psqlodbc-08.03.0400' : 'unixodbc-2.2.12',
                #'psqlodbc-08.02.0500' : 'unixodbc-2.2.12',
        }
        """
        valid_drivers = {}
        for driver in open(self._get_absolute_filename('valid_odbc_drivers.txt')):
            if driver[-1] == '\n':
                driver = driver[0:-1]
            if driver[0] == '#':
                continue
            if len(driver.split(',')) != 2:
                continue
            (driver_name, driver_manager_name) = driver.split(',')
            valid_drivers[driver_name] = driver_manager_name
        print valid_drivers
        if not os.environ['GPHOME_CONNECTIVITY']:
            return False
        driver_base_dir = os.environ['GPHOME_CONNECTIVITY'] + os.sep + 'drivers' + os.sep + 'odbc'
        for driver_name in os.listdir(driver_base_dir):
            
            if not valid_drivers.has_key(driver_name):
                continue
            driver_manager = valid_drivers[driver_name]
        
            #driver_manager = 'unixodbc-2.2.12'
            self.drivers.append((driver_name, driver_manager))

        self.connectivity_config_file = os.environ['GPHOME_CONNECTIVITY'] + os.sep + 'greenplum_connectivity_path.sh'
        if len(self.drivers) <= 0:
            return False
        return True

    def gen_odbcini_file(self, driver_name, driver_manager_name):
        driver = os.environ['GPHOME_CONNECTIVITY'] + os.sep + 'drivers' + os.sep + 'odbc' + os.sep + driver_name + os.sep + driver_manager_name + os.sep + 'psqlodbcw.so'
        dict = {
            'Driver' : driver,
            'Database' : self.pgdb,
            'Servername' : self.pghost,
            'UserName' : self.pguser,
            'Port' : self.pgport,
        }
        ofile = os.getcwd() + os.sep + 'odbc.ini' 
        os.system('rm -rf %s'%(ofile))
        f = open(ofile, 'w')
        for line in open(self._get_absolute_filename('odbc.ini.template')):
            if (len(line.split('=')) == 2):
                (k, v) = line.split('=')
                if dict.has_key(k):
                    f.write(k + '=' + dict[k] + '\n')
                else:
                    f.write(line)
            else:
                f.write(line)
        f.close()
        os.environ['ODBCINI'] = ofile
        #os.environ['ODBCSYSINI'] = os.getcwd()

    def config_odbc_driver(self, driver_name, driver_manager_name):
        driver_key = 'GP_ODBC_DRIVER'
        driver_manager_key = 'GP_ODBC_DRIVER_MANAGER'
        p_driver = re.compile('^%s' % driver_key)
        p_driver_manager = re.compile('^%s' % driver_manager_key)
        for line in fileinput.input(self.connectivity_config_file, inplace=1):
            if p_driver_manager.match(line):
                print driver_manager_key + '=' + driver_manager_name
            elif p_driver.match(line):
                print driver_key + '=' + driver_name
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
            if res[0] and res[0] == 'PYTHONPATH':
                os.environ['PYTHONPATH'] = res[1]
            if res[0] and res[0] == 'LD_LIBRARY_PATH':
                os.environ['LD_LIBRARY_PATH'] = res[1]
            if res[0] and res[0] == 'DYLD_LIBRARY_PATH':
                os.environ['DYLD_LIBRARY_PATH'] = res[1]
            if res[0] and res[0] == 'LIBPATH':
                os.environ['LIBPATH'] = res[1]
        proc.communicate()

    def compileGCC(self, filename, driver_name, driver_manager_name):
        cur_path = os.getcwd()
        os.chdir(MYD)
        include_dir = os.environ['GPHOME_CONNECTIVITY'] + os.sep + 'drivers' + os.sep + 'odbc' + os.sep + driver_name + os.sep + driver_manager_name + os.sep + 'include'
        lib_dir = os.environ['GPHOME_CONNECTIVITY'] + os.sep + 'drivers' + os.sep + 'odbc' + os.sep + driver_name + os.sep + driver_manager_name
        if os.environ.has_key('GCCFLAGS'):
            if platform.platform().find('AIX') == 0:
                # AIX compile ODBC with GCCFLAGS
                cmd = 'gcc %s -o %s %s.c OdbcCommon.c -I%s -L%s %s/psqlodbcw.so' % (os.environ['GCCFLAGS'], filename, filename, include_dir, lib_dir, lib_dir)
            else:
                # non-AIX compile ODBC with GCCFLAGS
                cmd = 'gcc %s -o %s %s.c OdbcCommon.c -I%s -L%s -lodbc' % (os.environ['GCCFLAGS'], filename, filename, include_dir, lib_dir)
        else:
            if platform.platform().find('AIX') == 0:
                # AIX compile ODBC without GCCFLAGS
                cmd = 'gcc -o %s %s.c OdbcCommon.c -I%s -L%s %s/psqlodbcw.so' % (filename, filename, include_dir, lib_dir, lib_dir)
            else:
                # non-AIX compile ODBC without GCCFLAGS
                cmd = 'gcc -o %s %s.c OdbcCommon.c -I%s -L%s -lodbc' % (filename, filename, include_dir, lib_dir)
        print cmd
        (ret, output) = commands.getstatusoutput(cmd)
        print output
        os.chdir(cur_path)
        return ret

    def doTest(self, filename):
        for (driver_name, driver_manager_name) in self.drivers:
            cur_path = os.getcwd()
            os.chdir(MYD)
            self.gen_odbcini_file(driver_name, driver_manager_name)
            self.config_odbc_driver(driver_name, driver_manager_name)
            os.system('rm -rf %s'%(filename))
            self.compileGCC(filename, driver_name, driver_manager_name)
            cmd = './%s > %s.out 2>&1' % (filename, filename)
            (ret, output) = commands.getstatusoutput(cmd)
            os.chdir(cur_path)
            if ret != 0:
                return False
        return True

    def test_connect_success(self):
        self.assertTrue(self.doTest('TestOdbcConnSuccess'))

    def test_connect_fail(self):
        self.assertTrue(self.doTest('TestOdbcConnFail'))

    def test_create_and_drop_table(self):
        self.assertTrue(self.doTest('TestOdbcCreateAndDropTable'))
    
    def test_insert_sql(self):
        self.assertTrue(self.doTest('TestOdbcInsertSQL'))

    def test_select_sql(self):
        self.assertTrue(self.doTest('TestOdbcSelectSQL'))

    def test_transaction_commit(self):
        self.assertTrue(self.doTest('TestOdbcTransactionCommit'))

    def test_transaction_rollback(self):
        self.assertTrue(self.doTest('TestOdbcTransactionRollback'))
