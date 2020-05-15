from mpp.models import SQLTestCase
from mpp.models import SQLConcurrencyTestCase

class PxfMatchAStringFromStringSetGlob(SQLConcurrencyTestCase):
    """
    @db_name pxfautomation
    @concurrency 1
    @gpdiff True
    """
    sql_dir = 'sql'
    ans_dir = 'expected'
    out_dir = 'output'
