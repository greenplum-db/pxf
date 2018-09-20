package org.greenplum.pxf.automation.utils.jsystem.report;

import java.io.IOException;

import jsystem.framework.report.Reporter;
import jsystem.framework.report.Reporter.ReportAttribute;

import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.junit.Assert;

/**
 * JSystem Reporter wrapper to custom reports
 */
public abstract class ReportUtils {

	public static void report(Reporter jsystemReport, Class<?> contextClass, String message) {
		report(jsystemReport, contextClass, message, Reporter.PASS);
	}

	/**
	 * Getting JSystem reporter and use it to report to the various reports it hooked to (HTML, XML
	 * ...) If the reporter is null, or in silent mode, the reports will directed to System.out.
	 * 
	 * @param jsystemReport
	 * @param contextClass from which class reported
	 * @param message to report
	 * @param status use {@link Reporter} Reporter.FAIL or Reporter.PASS
	 */
	public static void report(Reporter jsystemReport, Class<?> contextClass, String message, int status) {
		String messageToReport = message;

		if (contextClass != null) {
			messageToReport = contextClass.getSimpleName() + " -> " + message;
		}
		if (jsystemReport != null && !jsystemReport.isSilent()) {
			jsystemReport.report(messageToReport, status);
		} else {
			System.out.println(messageToReport);
		}
		// solves JIRA:GPSQL-1797, when failing via Report, the ant formatter didn't recognize it as
		// failure. if status is FAIL, than also fail by junit Assert.
		if (status == Reporter.FAIL) {
			Assert.fail(messageToReport);
		}
	}

	/**
	 * Starts Report parent level. All reports from this point will be his Children.
	 * 
	 * @param jsystemReport
	 * @param contextClass from which class reported
	 * @param message
	 * @throws IOException
	 */
	public static void startLevel(Reporter jsystemReport, Class<?> contextClass, String message) throws IOException {
		startLevel(jsystemReport, contextClass, "", message);
	}

	/**
	 * Starts Report parent level. All reports from this point will be his Children.
	 * 
	 * @param jsystemReport
	 * @param contextClass
	 * @param additionalText additional text to add to class name
	 * @param message
	 * @throws IOException
	 */
	public static void startLevel(Reporter jsystemReport, Class<?> contextClass, String additionalText, String message) throws IOException {
		String reportMessage = contextClass.getSimpleName() + " " + additionalText + " -> " + message;

		if (jsystemReport != null && !jsystemReport.isSilent()) {
			jsystemReport.startLevel(reportMessage);
		} else {
			System.out.println(reportMessage);
		}
	}

	/**
	 * Closes the report level.
	 * 
	 * @param jsystemReport
	 * @throws IOException
	 */
	public static void stopLevel(Reporter jsystemReport) throws IOException {

		if (jsystemReport != null && !jsystemReport.isSilent()) {
			jsystemReport.stopLevel();
		}
	}

	public static void reportBold(Reporter jsystemReport, Class<?> contextClass, String message) {
		String reportMessage = contextClass.getSimpleName() + " -> " + message;

		if (jsystemReport != null && !jsystemReport.isSilent()) {
			jsystemReport.report(reportMessage, ReportAttribute.BOLD);
		} else {
			System.out.println(reportMessage);
		}
	}

	/**
	 * Reports HTML code
	 * 
	 * @param jsystemReport
	 * @param contextClass
	 * @param data
	 */
	public static void reportHtml(Reporter jsystemReport, Class<?> contextClass, String data) {
		if (jsystemReport != null && !jsystemReport.isSilent()) {
			jsystemReport.report(contextClass.getSimpleName() + " -> " + data, ReportAttribute.HTML);
		}
	}

	public static void reportTable(Table table) {
		System.out.println(table);
	}

	/**
	 * Reports href in HTML report, status id PASS
	 * 
	 * @param jsystemReport
	 * @param title
	 * @param contextClass
	 * @param data
	 */
	public static void reportHtmlLink(Reporter jsystemReport, String title, Class<?> contextClass, String data) {
		reportHtmlLink(jsystemReport, contextClass, title, data, Reporter.PASS);
	}

	/**
	 * Reports href in HTML report
	 * 
	 * @param jsystemReport
	 * @param contextClass
	 * @param title
	 * @param data
	 * @param status use {@link Reporter} Reporter.FAIL or Reporter.PASS
	 */
	public static void reportHtmlLink(Reporter jsystemReport, Class<?> contextClass, String title, String data, int status) {
		String titleMessage = contextClass.getSimpleName() + " -> " + title;

		if (jsystemReport != null && !jsystemReport.isSilent()) {
			jsystemReport.report(titleMessage, data, status, ReportAttribute.HTML);
		}
		// solves JIRA:GPSQL-1797, when failing via Report, the ant formatter didn't recognize it as
		// failure. if status is FAIL, than also fail by junit Assert.
		if (status == Reporter.FAIL) {
			Assert.fail(titleMessage);
		}
	}

	/**
	 * reports title between stars headlines
	 * 
	 * @param jsystemReport
	 * @param contextClass
	 * @param message
	 */
	public static void reportTitle(Reporter jsystemReport, Class<?> contextClass, String message) {
		String titleHeadline = getTitleHeadlineForMessage((contextClass.getSimpleName() + " -> " + message));
		report(jsystemReport, null, titleHeadline, Reporter.PASS);
		report(jsystemReport, contextClass, message, Reporter.PASS);
		report(jsystemReport, null, titleHeadline, Reporter.PASS);
	}

	/**
	 * Gets stars headline according to the message length
	 * 
	 * @param message
	 * @return string of stars matching the length of the message
	 */
	private static String getTitleHeadlineForMessage(String message) {
		StringBuilder sBuilder = new StringBuilder();
		for (int i = 0; i < message.length(); i++) {
			sBuilder.append("*");
		}
		return sBuilder.toString();
	}

	/**
	 * Reports on unsupported functionality using reporter
	 * 
	 * @param contextClass reporter class
	 * @param message to add to report
	 * @throws Exception
	 */
	public static void throwUnsupportedFunctionality(Class<?> contextClass, String message) throws Exception {
		throw new Exception(contextClass.getSimpleName() + " -> " + message + " is currently not supported");
	}
}
