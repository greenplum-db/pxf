from mpp.models import SQLConcurrencyTestCase
from mpp.models import SQLTestCase

class HcatalogAggregateQueries(SQLConcurrencyTestCase):
    """
    @product_version  gpdb: [2.0-]
    @db_name pxfautomation
    @concurrency 1
    @gpdiff True
    """
    sql_dir = 'sql'
    ans_dir = 'expected'
    out_dir = 'output'
