package org.greenplum.pxf.automation.components.cluster.installer.nodes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import org.greenplum.pxf.automation.components.common.ShellSystemObject;

/**
 * Represents any Node, using ssh connectivity.
 */
public abstract class Node extends ShellSystemObject {
	// supported services in the node
	private String services;

	@Override
	public void init() throws Exception {
		// do not pass local env vars to ssh connection
		setIgnoreEnvVars(true);
		super.init();
		// increase command timeout for long responses
		setCommandTimeout(ShellSystemObject._10_MINUTES);
	}

	public String getServices() {
		return services;
	}

	public void setServices(String services) {
		this.services = services;
	}

	/**
	 * @return Services string as List
	 */
	public List<String> getServicesList() {
		if (StringUtils.isEmpty(services)) {
			return new ArrayList<>();
		}
		return Arrays.asList(services.replace(" ", "").split(","));
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + ": " + getHost() + " " + getHostName() + ", Services: " + (StringUtils.isEmpty(services) ? "Not Exists" : services);
	}
}