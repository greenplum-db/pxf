from mpp.models import SQLTestCase
from mpp.models import SQLConcurrencyTestCase

class PxfHivePartitions30k(SQLConcurrencyTestCase):
    """
    @product_version  gpdb: [1.3.0.3-]
    @db_name pxfautomation
    @concurrency 1
    @gpdiff True
    """
    sql_dir = 'sql'
    ans_dir = 'expected'
    out_dir = 'output'
