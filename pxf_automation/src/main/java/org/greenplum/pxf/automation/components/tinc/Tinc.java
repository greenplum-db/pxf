package org.greenplum.pxf.automation.components.tinc;

import java.io.File;
import java.io.IOException;

import org.greenplum.pxf.automation.components.common.ShellSystemObject;
import org.greenplum.pxf.automation.components.common.cli.ShellCommandErrorException;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;

public class Tinc extends ShellSystemObject {

	private String tincFolder;
	private String tincTestsFolder;
	private String gphome;

	public Tinc() {

	}

	public Tinc(boolean silentReport) {
		super(silentReport);
	}

	@Override
	public void init() throws Exception {

		ReportUtils.startLevel(report, getClass(), "init");

		super.init();

		runCommand("cd " + gphome);
		runCommand("source greenplum_path.sh");
		runCommand("cd " + new File(tincFolder).getAbsolutePath());
		runCommand("source tinc_env.sh");
		runCommand("cd " + new File(tincTestsFolder).getAbsolutePath());
		runCommand("source tincrepo_env.sh");

		ReportUtils.stopLevel(report);
	}

	/**
	 * Runs Tinc test command
	 * 
	 * @param tincTestPath full path to Tinc python Test class (like pxf.regression.HiveTests)
	 * @param discover if true, run all tests under the tincTestPath, false means run only the
	 *            tincTestPath
	 * @throws IOException
	 * @throws ShellCommandErrorException if command fails
	 */
	public void runTinc(String tincTestPath, boolean discover) throws IOException,
			ShellCommandErrorException {

		ReportUtils.startLevel(report, getClass(), "Run Test: " + tincTestPath);

		// max timeout for waiting for tinc's answer will be 10 minuted, after this it considered as
		// failure. Not waiting the full timeout if answer arrives earlier.
		setCommandTimeout(_10_MINUTES);

		if (discover) {
			// get in path and run discover
			runCommand("cd " + tincTestPath);
			runCommand("tinc.py discover .");
		} else {
			// run only current test
			runCommand("tinc.py " + tincTestPath);
		}

		ReportUtils.stopLevel(report);
	}

	/**
	 * Runs Single Tinc Test command
	 * 
	 * @param tincTestPath - path to tinc test
	 * @throws ShellCommandErrorException
	 * @throws IOException
	 */
	public void runTest(String tincTestPath) throws IOException,
			ShellCommandErrorException {
		runTinc(tincTestPath, false);
	}

	/**
	 * Discovers and runs all Tinc tests in the path
	 * 
	 * @param tincTestPath to discover and run
	 * @throws ShellCommandErrorException
	 * @throws IOException
	 */
	public void runAllTests(String tincTestPath) throws IOException,
			ShellCommandErrorException {
		runTinc(tincTestPath, true);
	}

	public String getTincFolder() {
		return tincFolder;
	}

	public void setTincFolder(String tincFolder) {
		this.tincFolder = tincFolder;
	}

	public String getTincTestsFolder() {
		return tincTestsFolder;
	}

	public void setTincTestsFolder(String tincTestsFolder) {
		this.tincTestsFolder = tincTestsFolder;
	}

	public String getGphome() {
		return gphome;
	}

	public void setGphome(String gphome) {
		this.gphome = gphome;
	}
}