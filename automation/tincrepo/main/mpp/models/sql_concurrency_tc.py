import datetime
import os
from threading import Thread

import tinctest
from tinctest.models.concurrency import ConcurrencyTestCase

from mpp.models import SQLTestCase

@tinctest.skipLoading("Test model. No tests loaded.")
class SQLConcurrencyTestCase(SQLTestCase, ConcurrencyTestCase):
    """
	SQLConcurrencyTestCase consumes a SQL file, performs the psql on the same sql file using concurrent connections
        specified by the 'concurrency' metadata.
        Note the order of base classes in the above inheritance. With multiple inheritance, the order in which the base
        class constructors will be called is from left to right. We first have to call SQLTestCase constructor which
        will dynamically generate our test method which is a pre-requisite to call ConcurrencyTestCase.__init__ because
        it works out of the test method that SQLTestCase generates. If you swap the order, good luck with debugging.

        @metadata: gpdiff: If set to true, for every reptition of the query for every iteration, the output is matched
                           with expected output and the test is failed if there is a difference. If set to false, the 
                           output is not matched with expected output for any of the repetition/iteration (default: False)
                           Note: the default is different from the parent SQLTestCase class.
    
    """
    def _infer_metadata(self):
        super(SQLConcurrencyTestCase, self)._infer_metadata()
        # Default value of gpdiff is False
        if self._metadata.get('gpdiff') and self._metadata.get('gpdiff') == 'True':
            self.gpdiff = True
        else:
            self.gpdiff = False
    
    def run_test(self):
        """
        This implementation of run test takes care of running sql files concurrenctly based
        on the metadata concurrency and iterations.

        A sql file query01.sql with concurrency '5' will be run in 5 different external processes
        concurrently.

        If there are part sqls along with a sql file - for eg: query01.sql, query01_part1.sql,
        query01_part2.sql etc with each invocation will run all the part sqls concurrently in
        different threads in each of the external process started above.
        """
        tinctest.logger.trace_in()
        # Construct an out_file name based on the currentime
        # run the test and the parts of the test if it has
        # concurrently.
        # TODO - generate a more readable file name based
        # on the current execution context (using iteration and
        # thread id from ConcurrencyTestCase
        sql_file_list = self.form_sql_file_list()
        compare_files_list = []
        thread_list = []
        for sql_file in sql_file_list:
            now = datetime.datetime.now()
            timestamp = '%s%s%s%s%s%s%s'%(now.year,now.month,now.day,now.hour,now.minute,now.second,now.microsecond)
            out_file = sql_file.replace('.sql', '_' + timestamp + '.out')
            out_file = os.path.join(self.get_out_dir(), os.path.basename(out_file))
            ans_file = sql_file.replace('.sql', '.out')
            ans_file = os.path.join(self.get_ans_dir(), os.path.basename(ans_file))
            compare_files_list.append((out_file, ans_file))
            thread_name = "Thread_" + os.path.splitext(os.path.basename(sql_file))[0]
            thread = _SQLConcurrencyTestCaseThread(target=self.run_sql_file, name=thread_name, args=(sql_file,), kwargs={'out_file': out_file})
            thread.start()
            thread_list.append(thread)

        # Wait for all the runs to finish
        for thread in thread_list:
            thread.join()

        # Time to compare the results
        result = True

        if self.gpdiff:
            for compare_objects in compare_files_list:
                if self.verify_out_file(compare_objects[0], compare_objects[1]):
                    tinctest.logger.info("Found no differences between out_file %s and ans_file %s." % (compare_objects[0], compare_objects[1]))
                else:
                    tinctest.logger.error("Found differences between out_file %s and ans_file %s." % (compare_objects[0], compare_objects[1]))
                    result = False
        tinctest.logger.trace_out()
        return result

class _SQLConcurrencyTestCaseThread(Thread):
    """
    A subclass of Thread. We need this because we want to
    get the return value of the target function.
    """
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs={}, Verbose=None):
        """
        initialize a thread
        """
        Thread.__init__(self, group, target, name, args, kwargs, Verbose)
        self._return = None
        self._name = name

    def run(self):
        """
        run the thread and save the return value of the target function
        into selt._return
        """
        tinctest.logger.info("Started thread: %s" %self._name)
        if self._Thread__target is not None:
            self._return = self._Thread__target(*self._Thread__args,
                                                **self._Thread__kwargs)

    def join(self):
        """
        return the return value of target function after join
        """
        Thread.join(self)
        tinctest.logger.info("Finished thread: %s" %self._name)
        return self._return

