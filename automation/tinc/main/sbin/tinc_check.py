#! /usr/bin/env python

from unittest2.main import USAGE_AS_MAIN
import tinctest

# Run in default order
print "#" * 80
print "Running tests in default order"
tinctest.TINCTestProgram.USAGE = USAGE_AS_MAIN
tinctest.TINCTestProgram(module = None, 
                testLoader = tinctest.default_test_loader,
                testRunner = tinctest.TINCTestRunner, exit = False)
print "#" * 80

# First run it in reverse order
print "#" * 80
print "Running tests in reverse ordering"
tinctest.TINCTestProgram.USAGE = USAGE_AS_MAIN
tinctest.TINCTestProgram(module = None, 
                testLoader = tinctest.reverse_test_loader,
                testRunner = tinctest.TINCTestRunner, exit = False)
print "#" * 80

# Run it in randomized order
print "#" * 80
print "Running tests in random ordering"
tinctest.TINCTestProgram.USAGE = USAGE_AS_MAIN
tinctest.TINCTestProgram(module = None, 
                testLoader = tinctest.randomized_test_loader,
                testRunner = tinctest.TINCTestRunner, exit = False)
print "#" * 80

