package reporters;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.testng.IReporter;
import org.testng.ISuite;
import org.testng.ISuiteResult;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.xml.XmlSuite;

import annotations.ExpectedFailure;

/**
 * Implementation for TestNg {@link IReporter} which write all the "known failures" cases into a
 * file located in the given outputDirectory
 */
public class CustomAutomationReport implements IReporter {

	@Override
	public void generateReport(List<XmlSuite> xmlSuites, List<ISuite> suites, String outputDirectory) {

		StringBuilder sBuilder = new StringBuilder();

		// Iterating over each suite
		for (ISuite suite : suites) {

			// Getting the results for the each suite
			Map<String, ISuiteResult> suiteResults = suite.getResults();
			for (ISuiteResult sr : suiteResults.values()) {
				// get context for result
				ITestContext tc = sr.getTestContext();
				// go over all cases
				for (ITestNGMethod method : tc.getAllTestMethods()) {
					// if a case has "ExpectedFaiGPDBWritable.javalure" annotation, insert to sBuilder

					if (method.getConstructorOrMethod().getMethod().getAnnotation(ExpectedFailure.class) != null) {
						sBuilder.append(method.getInstance().getClass().getName() + "/" + method.getMethodName()).append(System.lineSeparator());
					}
				}
			}
		}

		// write results to file
		try {
			FileUtils.writeStringToFile(new File(outputDirectory + "/automation_expected_failures.txt"), sBuilder.toString(), false);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
