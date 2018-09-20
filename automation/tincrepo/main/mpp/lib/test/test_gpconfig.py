#! /usr/bin/env python

import unittest2 as unittest


from mpp.lib.gpConfig import GpConfig


class GpConfigTests(unittest.TestCase):
    def setUp(self):
        self.gp_config = GpConfig()

    #HK: disabling test 
    #can't test with hardcoded ports
    #need better handling or remove test
    def donttest_getPort(self):
        port_info = self.gp_config.getPort()
        expected = {0: 18506, 1: 18507, 2: 18508, -1: 18501}
        self.assertEquals(port_info, expected)

    def test_getParameter(self):
        name = "port"
        self.assertRaises(Exception,self.gp_config.getParameter, name)

    #HK: disabling test 
    #these params apparently require restart to actually take effect
    def donttest_set_get_verify_remove_ParameterMasterOnly(self):
        name = "max_connections"
        master_value = "100" 
        self.gp_config.setParameterMasterOnly(name, master_value)
        self.assertTrue(self.gp_config.verifyParameterMasterOnly(name, master_value))
        self.gp_config.removeParameterMasterOnly(name)
        self.assertRaises(self.gp_config.getParameterMasterOnly(name))

    #HK: disabling test 
    #these params apparently require restart to actually take effect
    def donttest_set_get_verify_remove_Parameter(self):
        name = "max_connections"
        master_value = "100"
        seg_value = "1000"
        self.gp_config.setParameter(name, master_value, seg_value)
        self.assertTrue(self.gp_config.verifyParameter(name, master_value, seg_value))
        self.gp_config.removeParameter(name)
        self.assertRaises(self.gp_config.getParameter(name))

    #HK: disabling test 
    #these params apparently require restart to actually take effect
    def donttest_set_get_verify_remove_ParameterSegmentOnly(self):
        name = "max_connections"
        seg_value = "1000"
        self.gp_config.setParameterSegmentOnly(name, seg_value)
        self.assertTrue(self.gp_config.verifyParameterSegmentOnly(name, seg_value))
        self.gp_config.removeParameterSegmentOnly(name)
        self.assertRaises(self.gp_config.getParameterSegmentOnly(name))


if __name__ =="__main__":
    unittest.main()
