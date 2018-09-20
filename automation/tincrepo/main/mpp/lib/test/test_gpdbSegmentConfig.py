#! /usr/bin/env python

import unittest2 as unittest
import os

from mpp.lib.gpdbSegmentConfig import GpdbSegmentConfig

from tinctest.lib import run_shell_command

class GpdbSegmentConfigTests(unittest.TestCase):
    gpdbSegmentConfig = GpdbSegmentConfig()

    @classmethod
    def setUpClass(cls):
        # Hardcoding this to make these tests pass.
        # The library has a hardcoded dbname which has to be changed.
        run_shell_command("createdb gptest")
        
    @classmethod
    def tearDownClass(cls):
        run_shell_command("dropdb gptest")

    def test_GetSqlData(self):
        (rc,data) = self.gpdbSegmentConfig.GetSqlData('')
        expected = (0,[])
        self.assertEquals((rc,data),expected)
    def test_hasStandbyMaster(self):
        ret = self.gpdbSegmentConfig.hasStandbyMaster()
        expected = False
        self.assertEquals(ret,expected)
    def test_hasMirrors(self):
        ret = self.gpdbSegmentConfig.hasMirrors()
        expected = False
        self.assertEquals(ret,expected)
    def test_GetSegmentInvalidCount(self):
        (rc,count) = self.gpdbSegmentConfig.GetSegmentInvalidCount()
        expected = (0,'0')
        self.assertEquals((rc,count),expected)
    def test_GetSegmentInSync(self):
        (rc,inSync) = self.gpdbSegmentConfig.GetSegmentInSync(repeatCnt=0)
        expected = (0,False)
        self.assertEquals((rc,inSync),expected)

    def test_GetMasterHost(self):
        (rc,data) = self.gpdbSegmentConfig.GetMasterHost()
        expected = (0,None)
        self.assertEquals((rc,data),expected)
    def test_GetServerList(self):
        (rc,data) = self.gpdbSegmentConfig.GetServerList()
        expected = (0,[])
        self.assertEquals((rc,data),expected)
    def test_GetMasterStandbyHost(self):
        (rc,data) = self.gpdbSegmentConfig.GetMasterStandbyHost()
        expected = (0,None)
        self.assertEquals((rc,data),expected)
    def test_GetHostAndPort(self):
        (rc,data1,data2) = self.gpdbSegmentConfig.GetHostAndPort('')
        expected = (0,"","")
        self.assertEquals((rc,data1,data2),expected)
    def test_GetContentIdList(self):
        contentList = self.gpdbSegmentConfig.GetContentIdList()
        expected = []
        self.assertEquals(contentList,expected)
    def test_GetMasterDataDirectory(self):
        datadir = self.gpdbSegmentConfig.GetMasterDataDirectory()
        expected = ''
        self.assertEquals(datadir,expected)
    def test_GetSegmentData(self):
        segmentData = self.gpdbSegmentConfig.GetSegmentData(1)
        expected = []
        self.assertEquals(segmentData,expected)

if __name__ == "__main__":

    unittest.main()
