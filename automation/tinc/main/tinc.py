#! /usr/bin/env python

from unittest2.main import USAGE_AS_MAIN
import tinctest

tinctest.TINCTestProgram.USAGE = USAGE_AS_MAIN
tinctest.TINCTestProgram(module = None, 
                testLoader = tinctest.default_test_loader,
                testRunner = tinctest.TINCTestRunner)
