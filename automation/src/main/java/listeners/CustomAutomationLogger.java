package listeners;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

import annotations.ExpectedFailure;

/**
 * TestNg {@link ITestListener}, has method for each run time events during scenario run.<br>
 * This {@link CustomAutomationLogger} is redirecting all stdout output to files located in
 * "LOG_DIRECTORY/<Test Class Name>/<time stamp>_<Test Method Name>.log" At the end of each test
 * case run, reporting the result to the console.
 */
public class CustomAutomationLogger implements ITestListener {

	// will store original PrintStream from System.out
	private static PrintStream originalStdOutStream;
	// reference to current file stream
	private static PrintStream fileStream;
	// base log directory
	private static final String LOGS_DIRECTORY = "automation_logs";

	/**
	 * Called when the test-method execution starts
	 */
	@Override
	public void onTestStart(ITestResult result) {
		try {
			// redirect System.out to file stream to collect all output to a file
			redirectStdoutStreamToFile(getClassSimpleName(result), getTestMethodName(result));
		} catch (FileNotFoundException e) {
			System.out.println("CustomAutomationLogger: Error redirecting Stream to file: " + e.getMessage());
		}
	}

	/**
	 * Called when the test-method execution is a success
	 */
	@Override
	public void onTestSuccess(ITestResult result) {
		alertEvent(result);
	}

	/**
	 * Called when the test-method execution fails
	 */
	@Override
	public void onTestFailure(ITestResult result) {
		// write exception stack trace to the file stream
		result.getThrowable().printStackTrace(System.out);

		// send to alertEvent true, if method has "ExpectedFailure" annotation
		boolean expectedToFail = (result.getMethod().getConstructorOrMethod().getMethod().getAnnotation(ExpectedFailure.class) != null);
		alertEvent(result, expectedToFail);
	}

	/**
	 * Called when the test-method skipped
	 */
	@Override
	public void onTestSkipped(ITestResult result) {
		// write "SKIPPED" to file stream
		System.out.println("SKIPPED");
		alertEvent(result);
	}

	/**
	 * Called when the test-method fails within success percentage
	 */
	@Override
	public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
		// Leaving blank for future use
	}

	/**
	 * Called when the suite starts
	 */
	@Override
	public void onStart(ITestContext context) {
		// Leaving blank for future use
	}

	/**
	 * Called when the test in xml suite finishes
	 */
	@Override
	public void onFinish(ITestContext context) {
		// Leaving blank for future use
	}

	/**
	 * get the Test Class Simple name from given {@link ITestResult}
	 * 
	 * @param result {@link ITestResult}
	 * @return Simple name of test class
	 */
	private String getClassSimpleName(ITestResult result) {

		if (result == null) {
			return null;
		}

		// split according to '.' and grab the last split as the class simple name
		String[] classNameSplit = result.getTestClass().getName().split("\\.");
		String classSimpleName = classNameSplit[classNameSplit.length - 1];

		return classSimpleName;
	}

	/**
	 * get test method name from given {@link ITestResult}
	 * 
	 * @param result {@link ITestResult}
	 * @return test method name
	 */
	private String getTestMethodName(ITestResult result) {

		if (result == null) {
			return null;
		}

		// return the name from the test result which is the test-case method
		return result.getName();
	}

	/**
	 * return <Test Class Name>.<Test Method name> prefix for given {@link ITestResult}
	 * 
	 * @param result {@link ITestResult}
	 * @return test name prefix
	 */
	private String getTestNamePrefix(ITestResult result) {
		return getClassSimpleName(result) + "." + getTestMethodName(result);
	}

	/**
	 * flush and close current file stream and redirect System.out back to original stream.
	 */
	public static void revertStdoutStream() {
		fileStream.flush();
		fileStream.close();
		System.setOut(originalStdOutStream);
	}

	/**
	 * Redirects System.out to a file stream, the file will be created in <br>
	 * "LOG_DIRECTORY/<test class name>/<time stamp>_<test method name>.log"
	 * 
	 * @param testClass given test class name
	 * @param testMethod given test method name
	 * @throws FileNotFoundException if file or directories not exists while trying to open stream
	 *             to it
	 */
	public static void redirectStdoutStreamToFile(String testClass, String testMethod) throws FileNotFoundException {

		// define file to required file
		File logFile = new File(LOGS_DIRECTORY + "/" + testClass + "/" + System.currentTimeMillis() + "_" + testMethod + ".log");
		// define file to it parent directory
		File parentDirectory = logFile.getParentFile();

		// make sure all directories exists before creating the stream
		if (!parentDirectory.exists()) {
			parentDirectory.mkdirs();
		}

		// save original System.out stream
		originalStdOutStream = System.out;
		// create new file stream to the above file
		fileStream = new PrintStream(logFile.getAbsolutePath());
		// set stream to System.out
		System.setOut(fileStream);
	}

	private void alertEvent(ITestResult testResult) {
		alertEvent(testResult, false);
	}

	/**
	 * alert result of test case to the console in the following format:<br>
	 * <test name prefix>...<event message>
	 * 
	 * @param testResult
	 */
	private void alertEvent(ITestResult testResult, boolean expectedToFail) {
		String eventMessage = "";
		// put the right event message to eventMessage according to testResult.getStatus()
		switch (testResult.getStatus()) {
			case ITestResult.SUCCESS:
				eventMessage = "OK";
				break;
			case ITestResult.FAILURE:
				// if case expected to fail, sign as "EXPECTED FAILURE"
				if (!expectedToFail) {
					eventMessage = "FAILED";
				} else {
					eventMessage = "EXPECTED FAILURE";
				}
				break;
			case ITestResult.SKIP:
				eventMessage = "SKIPPED";
				break;
			default:
				break;
		}

		// redirect System.out back to original stream
		revertStdoutStream();
		// alert event to console
		System.out.println(getTestNamePrefix(testResult) + "..." + eventMessage);
	}
}