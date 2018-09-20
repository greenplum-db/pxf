package org.greenplum.pxf.automation.components.cluster.installer.nodes;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import org.greenplum.pxf.automation.components.common.cli.ShellCommandErrorException;

/**
 * Represents the node that running the installation of the cluster
 */
public class InstallationNode extends Node {
	// server for downloading the required builds
	private String downloadServer = "http://dist.dh.greenplum.com/dist/PHD/testing/";
	// path to resources in the installation node
	private String resourceDirectory = "src/test/resources/templates/";

	/**
	 * Get the latest build for given buildName
	 * 
	 * @param buildName required build type
	 * @return latest build from given buildName
	 * @throws IOException
	 * @throws ShellCommandErrorException
	 * @throws ConfigurationException
	 */
	private String getLatestBuilds(final String buildName) throws IOException, ShellCommandErrorException, ConfigurationException {
		// connect to server and get list of all files
		Document doc = Jsoup.connect(downloadServer).get();
		// select only the match files according to buildName
		Elements el = doc.select("a[href]:matches(" + buildName + "-\\d+)");

		// sort files elements
		Collections.sort(el, new Comparator<Element>() {
			@Override
			public int compare(Element e1, Element e2) {
				// leave only the build number X (1.2.0.1-X) and compare it as integer
				return Integer.valueOf(e1.text().replaceAll(buildName + "-", "").replaceAll(".tar.gz", "")).compareTo(Integer.valueOf(e2.text()
						.replaceAll(buildName + "-", "")
						.replaceAll(".tar.gz", "")));
			}
		});

		// return latest build name
		return el.last().text().replaceAll(".tar.gz", "");
	}

	/**
	 * Get the required version for given buildPattern: if a fixed version is given than return it,
	 * "<build type>-<version>-" mean that the latest from the given version will be returned.
	 * 
	 * @param buildPattern required build and version
	 * @return required build according to given buildPattern
	 * @throws ConfigurationException
	 * @throws IOException
	 * @throws ShellCommandErrorException
	 */
	public String getRequiredVersion(String buildPattern) throws ConfigurationException, IOException, ShellCommandErrorException {
		// if buildPattern is empty return null
		if (!StringUtils.isEmpty(buildPattern)) {
			// if ends with "-"return the latest from required version, else return fixed build
			if (buildPattern.endsWith("-")) {
				return getLatestBuilds(buildPattern.substring(0, buildPattern.length() - 1));
			} else {
				return buildPattern;
			}
		}
		return null;
	}

	public String getDownloadServer() {
		return downloadServer;
	}

	public void setDownloadServer(String downloadServer) {
		this.downloadServer = downloadServer;
	}

	public String getResourceDirectory() {
		return resourceDirectory;
	}

	public void setResourceDirectory(String resourceDirectory) {
		this.resourceDirectory = resourceDirectory;
	}
}