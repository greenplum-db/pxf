from mpp.lib.datagen import TINCTestDatabase
from mpp.lib.datagen import TINCDatagenException

# Global database dict for supported databases. Each test case will be associated with a
# specific database generator whose setup will be called during the setup of a test case.
TINC_TEST_DATABASE = 'gptest'

__databases__ = {}

__databases__[TINC_TEST_DATABASE] = TINCTestDatabase(database_name=TINC_TEST_DATABASE)
