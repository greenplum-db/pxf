from mpp.models import SQLConcurrencyTestCase
from mpp.models import SQLTestCase

class Writable(SQLConcurrencyTestCase):
    """
    @product_version  hawq: [2.0-]
    @db_name pxfautomation
    @concurrency 1
    @gpdiff True
    """
    sql_dir = 'sql'
    ans_dir = 'expected'
    out_dir = 'output'
