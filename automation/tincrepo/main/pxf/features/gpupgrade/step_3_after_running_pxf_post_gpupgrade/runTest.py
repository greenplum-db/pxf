from mpp.models import SQLTestCase
from mpp.models import SQLConcurrencyTestCase

class PxfGpupgradeStep3AfterRunningPxfPostGpupgrade(SQLConcurrencyTestCase):
    """
    @db_name pxfautomation
    @concurrency 1
    @gpdiff True
    """
    sql_dir = 'sql'
    ans_dir = 'expected'
    out_dir = 'output'