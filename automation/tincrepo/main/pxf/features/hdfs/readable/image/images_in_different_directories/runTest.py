from mpp.models import SQLTestCase
from mpp.models import SQLConcurrencyTestCase

class PxfHdfsImageTestImagesInDifferentDirectories(SQLConcurrencyTestCase):
    """
    @product_version  gpdb: [1.3.1-]
    @db_name pxfautomation
    @concurrency 1
    @gpdiff True
    """
    sql_dir = 'sql'
    ans_dir = 'expected'
    out_dir = 'output'
