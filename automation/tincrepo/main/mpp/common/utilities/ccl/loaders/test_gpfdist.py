#!/usr/bin python

import os
import commands
import socket
import subprocess
import signal
import time
import tinctest

class CCLGpfdistTestCase(tinctest.TINCTestCase):
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

    def startGpfdistAndReturnSubprocessObject(self):
        MYD = os.path.abspath(os.path.dirname(__file__))
        data_path = MYD + os.sep + 'data'
        p = subprocess.Popen(["gpfdist","-d", data_path])
        return p

    def get(self, rfile, protocol=0):
        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        # TODO select port dynamiclly
        s.connect(('127.0.0.1',8080))
        s.sendall('GET %s HTTP/1.0\r\nX-GP-PROTO: %d\r\nX-GP-CSVOPT: m0x0q0h1\r\n\r\n' %  (rfile,protocol))
        g = s.makefile()
        s.close()
        ret = ''
        for line in g:
            ret += line
        g.close()
        return ret

    def test_gpfdist_simple(self):
        p = self.startGpfdistAndReturnSubprocessObject()
        try:
            time.sleep(1)
            res_header = self.get('test.txt')
            ret = (res_header.lower().find('http/1.0 200 ok') >= 0)
            self.assertTrue(ret)
        finally:
            os.kill(p.pid, signal.SIGKILL)
            time.sleep(1)
