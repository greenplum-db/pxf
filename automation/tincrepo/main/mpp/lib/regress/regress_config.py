import os
import socket
import unittest2 as unittest
from mpp.lib.config import GPDBConfig

class GPDBConfigRegressionTests(unittest.TestCase):

    def __init__(self, methodName):
        self.gpconfig = GPDBConfig()
        super(GPDBConfigRegressionTests,self).__init__(methodName)

    def test_get_countprimarysegments(self):
        nprimary = self.gpconfig.get_countprimarysegments()
        self.assertTrue(nprimary > 0)

    def test_get_hostandport_of_segment(self):
        (host,port) = self.gpconfig.get_hostandport_of_segment(psegmentNumber = -1, pRole = 'p')
        myhost = socket.gethostname()
        self.assertEquals(host, myhost)

    def test_get_count_segments(self):
        seg_count = self.gpconfig.get_count_segments()
        self.assertTrue(seg_count.strip() >0)

    def test_seghostnames(self):
        hostlist = self.gpconfig.get_hosts()
        self.assertTrue(len(hostlist) >0)
 
    def test_hostnames(self):
        hostlist = self.gpconfig.get_hosts(segments=False)
        self.assertTrue(len(hostlist) >0)

    def tes_get_masterhost(self):
        master_host = self.gpconfig.get_masterhost()
        myhost = socket.gethostname()
        self.assertEquals(master_host, myhost)

    def test_get_masterdata_directory(self):
        master_dd = self.gpconfig.get_masterdata_directory()
        my_mdd = os.getenv("MASTER_DATA_DIRECTORY")
        self.assertEquals(master_dd, my_mdd)


