import os
import shutil

import unittest2 as unittest

from tinctest.lib.system import TINCSystem, TINCSystemException

class TINCSystemTests(unittest.TestCase):
    
    def test_make_dirs(self):
        test_dir = os.path.join(os.path.dirname(__file__), 'test_mkdirs')
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)
        self.assertFalse(os.path.exists(test_dir))
        TINCSystem.make_dirs(test_dir)
        self.assertTrue(os.path.exists(test_dir))

    def test_make_dirs_existing(self):
        test_dir = os.path.join(os.path.dirname(__file__), 'test_mkdirs')
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)
        self.assertFalse(os.path.exists(test_dir))
        TINCSystem.make_dirs(test_dir)
        self.assertTrue(os.path.exists(test_dir))
         
        # This should fail
        with self.assertRaises(OSError) as cm:
            TINCSystem.make_dirs(test_dir)

    def test_make_dirs_existing_ignore(self):
        test_dir = os.path.join(os.path.dirname(__file__), 'test_mkdirs')
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)
        self.assertFalse(os.path.exists(test_dir))
        TINCSystem.make_dirs(test_dir)
        self.assertTrue(os.path.exists(test_dir))
         
        # This should not fail and ignore existing error
        TINCSystem.make_dirs(test_dir, ignore_exists_error = True)

    def test_make_dirs_relative_path(self):
        test_dir = 'test/output'
        with self.assertRaises(TINCSystemException) as cm:
            TINCSystem.make_dirs(test_dir)        
       
        
   
