from mpp.gpdb.lib.models.sql import SQLPerformanceTestCase
from mpp.gpdb.lib.models.sql.optimizer import OptimizerSQLPerformanceTestCase

class BaseTPCHPlannerPerformanceTestCase(SQLPerformanceTestCase):
    """
    metadata here can be applied to all the tpch planner test cases.
    """

    def _infer_metadata(self):
        self._append_docstrings(BaseTPCHPlannerPerformanceTestCase.__doc__)
        super(BaseTPCHPlannerPerformanceTestCase, self)._infer_metadata()

class BaseTPCHOptimizerPerformanceTestCase(OptimizerSQLPerformanceTestCase):
    """
    metadata here can be applied to all the tpch optimizer test cases
    except tpch_small_scale which is not inherited from this:
    """

    def _infer_metadata(self):
        self._append_docstrings(BaseTPCHOptimizerPerformanceTestCase.__doc__)
        super(BaseTPCHOptimizerPerformanceTestCase, self)._infer_metadata()
