import os
import socket
import time

import unittest2 as unittest

import tinctest



class TINCTestRunner(unittest.TextTestRunner):
    """
    The default test runner used in tinc. This makes sure we use TINC specific
    result objects with tinc runs. By default, unittest's test runner uses a
    unit test result object and we override some internal methods in this class
    to patch a TINCTestResultSet for a TINCTestSuite.
    """
    def _makeResult( self ):
        return TINCTestResultSet(self.stream, self.descriptions, self.verbosity)


class TINCTextTestResult(unittest.TextTestResult):
    """
    TINC test result class that can print formatted text results to a stream.
    Our normal and verbose mode is verbose mode in unittest and our quiet mode is normal mode in unittest,
    hence the verbosity value is increased.
    """
    def __init__(self, stream, descriptions, verbosity):
        super(TINCTextTestResult, self).__init__(stream, descriptions, verbosity+1)
        self.start_time = 0
        self.end_time = 0
        self.duration = ''
        self.result_string = ''
        self.result_message = ''
        self.value = 0
        self.result_detail = {}
        self.output = ''
        self.jiras = ''
        self.stack_trace = ''
        self.tinc_normal_output = verbosity == 1
        self.verbosity = verbosity

    def getDescription(self, test):
        """
        Return a brief description of the test object as a string

        @param test: An instance of TINCTestCase
        @type test: TINCTestCase

        @rtype: string
        @return: A brief description of the test case. Uses TINCTestCase's description variable.
        """
        method_name, full_name = [name.strip('*') for name in test.__str__().split()]
        suite_name, class_name = full_name.strip('()').rsplit('.',1)
        case_name = class_name + '.' + method_name
        if self.tinc_normal_output:
            return case_name
        return '%s (%s) "%s"' % (case_name, suite_name, test.description)

    def startTest(self, test):
        """
        This implementation of startTest sets start time for test objects.

        @param test: An instance of TINCTestCase
        @type test: TINCTestCase
        """
        super(TINCTextTestResult, self).startTest(test)
        self.start_time = test.start_time = time.time()
        tinctest.logger.separator()
        tinctest.logger.status("Started test: %s : %s" %(test.full_name, test.description))

    def stopTest(self, test):
        """
        This implementation of stopTest sets the result_message instance variable for failed / errored tests.

        @param test: An instance of TINCTestCase
        @type test: TINCTestCase
        """
        if self.stack_trace:
            tinctest.logger.exception("Stack trace: %s" %self.stack_trace)
        result_message = self.result_message if self.result_message else "NONE"
        tinctest.logger.status("Finished test: %s Result: %s Duration: %s Message: %s" %(test.full_name,
                                                                                       self.result_string,
                                                                                       self.duration,
                                                                                       result_message))
        tinctest.logger.separator()
        super(TINCTextTestResult, self).stopTest(test)

    def addSuccess(self, test):
        self.end_time = test.end_time = time.time()
        self.result_string = 'PASS'
        self._show_run_time(test)
        super(TINCTextTestResult, self).addSuccess(test)

    def addError(self, test, err):
        self.result_string = 'ERROR'
        self.result_message = err[1]
        self._collect_files(test)
        self.stack_trace = self._exc_info_to_string(err, test).strip()
        self._show_run_time(test)
        super(TINCTextTestResult, self).addError(test, err)

    def addFailure(self, test, err):
        self.result_message = err[1]
        self.stack_trace = self._exc_info_to_string(err, test).strip()
        if self.stack_trace.find(tinctest._SKIP_TEST_MSG_PREFIX) != -1:
            # if we are in addFailure and see a message for skipped test
            # in the message, it is coming a thread (eg, concurrenty test)
            # handle it separately
            pass
        else:
            self.result_string = 'FAIL'
            self._collect_files(test)
            self._show_run_time(test)
            if tinctest.main.TINCTestProgram.tinc_config is not None and tinctest.main.TINCTestProgram.build_config is not None:
                self._create_issue(test, err, tinctest.main.TINCTestProgram.tinc_config, tinctest.main.TINCTestProgram.build_config)
            super(TINCTextTestResult, self).addFailure(test, err)

    def addExpectedFailure(self, test, err):
        self.end_time = test.end_time = time.time()
        self.result_string = 'PASS'
        self._show_run_time(test)
        super(TINCTextTestResult, self).addExpectedFailure(test, err)

    def addUnexpectedSuccess(self, test):
        self.end_time = test.end_time = time.time()
        self.result_string = 'FAIL'
        self.result_message = 'Unexpected success for the test marked as expected failure'
        self._show_run_time(test)
        super(TINCTextTestResult, self).addUnexpectedSuccess(test)

    def _create_issue(self, test, err, tinc_config, build_config):
        """
        Encapsulates the default logic to file a JIRA on a test failure.
        File an MPP by default and add test.test_artifacts as attachments.
        Override this method in subclasses to implement custom filing of JIRAs.
        """
        if (not tinc_config.enable_jira_filing) or (not test.enable_jira_filing):
            tinctest.logger.warning("JIRA filing is disabled for this run. Enable / disable this feature in the config file")
            return
        mpp_issue_dict = self._create_mpp_issue_dict(test, err, tinc_config, build_config)

        test_name = '%s.%s.%s' %(test.__class__.__module__, test.__class__.__name__, test._testMethodName)
        # De-dup JIRA based on test name and affectsVersion
        # Find if there is an Open JIRA already associated with this particular test case.
        # If yes, add a comment to the open JIRA , otherwise file a new JIRA
        summary_terms = mpp_issue_dict['summary'].split(" ")
        jql_string = ''
        for term in summary_terms:
            jql_string += 'summary ~ \"%s\" AND ' %(term.strip(':'))
        # Add assignee
        jql_string += ' reporter = \"%s\"' %(tinc_config.jira_user)
        # Add version
        jql_string += ' AND affectedVersion = \"%s\"' %mpp_issue_dict['affectsVersions']
        # Add status = open
        jql_string += ' AND status = \"open\"'

        tinctest.logger.info("Creating issue for test failure - %s" %test_name)
        tinctest.logger.info("JIRA isue dict - %s" %mpp_issue_dict)

        try:
            client1 = JIRAClient(jira_user=tinc_config.jira_user,
                                 jira_password=tinc_config.jira_password,
                                 jira_url=tinc_config.jira_url,
                                 logger = tinctest.logger)
            test_issues = client1.search_issues_jql(jql_string)

            comment_string = "TINC Comment: Test failed in another run. %s \n Environment: \n \
                              {code} %s {code} \n Failure Information: %s" %(test_name,
                                                                             mpp_issue_dict['environment'],
                                                                             mpp_issue_dict['description'])
            dedup_match = len(test_issues) > 0
            for issue in test_issues:
                tinctest.logger.info("Found an existing JIRA %s for the test" %issue)
                self.jiras = issue + ' ,'
                client1.add_comment(issue, comment_string)

            # Return if there are existing open JIRAs for the test
            if dedup_match:
                return

            issue_id = client1.create_issue(template='MPP', issue_dict=mpp_issue_dict)
            tinctest.logger.info("Filed JIRA - %s" %issue_id)
            self.jiras = issue_id
            for artifact in test.test_artifacts:
                tinctest.logger.info("Attaching artifact %s to JIRA %s" %(artifact, issue_id))
                client1.add_attachment(issue_id, artifact)
        except Exception, ex:
            tinctest.logger.warning("Exception while performing JIRA operation - %s" %ex)

    def _create_mpp_issue_dict(self, test, err, tinc_config, build_config):
        test_name = '%s.%s.%s' %(test.__class__.__module__, test.__class__.__name__, test._testMethodName)
        # Create an MPP issue
        mpp_issue_dict = {}
        mpp_issue_dict['summary'] = 'TINC test failure: %s' %test_name
        mpp_issue_dict['assignee'] = tinc_config.jira_user
        # Need to figure out a way to find the components for a test failure.
        # For now defaulting the component to 'Query Execution'
        # Automatically filed JIRAs will require a triage to find the correct component
        # based on a failure.
        mpp_issue_dict['components'] = 'Query Execution'
        # Get affects version from build info file
        mpp_issue_dict['affectsVersions'] = build_config.build_version
        # Get environment from build info file
        mpp_issue_dict['environment'] = "%s \n Hostname: %s \n Username: %s. \n Source the appropriate env file." %(build_config.build_platform,
                                                                                                                    socket.gethostname(),
                                                                                                                    os.environ['USER'])
        mpp_issue_dict['description'] = "%s \n TINC Stack Trace: \n {code}%s{code}" %(mpp_issue_dict['summary'], self.stack_trace)
        mpp_issue_dict['reproduction'] = 'tinc.py %s' %test_name
        mpp_issue_dict['project'] = 'MPP'
        mpp_issue_dict['debugging'] = 'See attached test artifacts'
        return mpp_issue_dict

    def addSkip(self, test, err):
        self.result_string = 'SKIP'
        self.result_message = err
        self._show_run_time(test)
        super(TINCTextTestResult, self).addSkip(test, err)

    def _show_run_time(self, test):
        self.end_time = test.end_time = time.time()
        elapsed = self.end_time - self.start_time
        self.duration = test.duration = "%4.2f ms" %(elapsed*1000)
        if not self.dots:
            self.stream.write("%s ... " %self.duration)

    def _collect_files(self, test):
        if hasattr(test, 'collect_files'):
            test.collect_files()

class TINCTestResultSet(TINCTextTestResult):
    """
    A default test result set object that will be used with TINCTestRunner for running
    an instance of a TINCTestSuite.
    """

    def __init__(self, stream, descriptions, verbosity):
        self.run_results = list()
        super(TINCTestResultSet, self).__init__(stream, descriptions, verbosity)

    def addResult(self, result):
        self.run_results.append(result)
        self.testsRun += result.testsRun
        if result.skipped:
            self.skipped.extend(result.skipped)
        if result.failures:
            if result.__class__.__name__ == 'TestResult':
                self._print_fail_summary(result.failures[0], 'FAIL')
            self.failures.extend(result.failures)
        if result.errors:
            if result.__class__.__name__ == 'TestResult':
                self._print_fail_summary(result.errors[0], 'ERROR')
            self.errors.extend(result.errors)
        if result.expectedFailures:
            self.expectedFailures.extend(result.expectedFailures)
        if result.unexpectedSuccesses:
            self.unexpectedSuccesses.extend(result.unexpectedSuccesses)

    def _print_fail_summary(self, res_tuple, fail_type = 'ERROR'):
        '''
        QAINF-875
        When non-test case specific errors/failures (they are errors mainly) occur,
        unittest constructs a new test object for the error (ModuleImportError object,
        for example) in the result. If we get here, it is because of the existence
        of such a "test". In order to output some status that pulse can parse, we
        are going to use this function. We will assume time taken to be 0ms.
        '''
        self.stream.write(res_tuple[0]._testMethodName)
        self.stream.write(" ... ")
        self.stream.write("0.0 ms")
        self.stream.write(" ... ")
        self.stream.writeln(fail_type)
        tinctest.logger.status("Finished test: %s Result: %s Duration: %s Message: %s" %(res_tuple[0]._testMethodName,
                                                                                       fail_type,
                                                                                       "0.00 ms",
                                                                                       res_tuple[1]))
