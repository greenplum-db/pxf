package com.pxf.automation.components.cluster.installer.services;

import jsystem.framework.system.SystemObjectImpl;

import com.pxf.automation.components.cluster.installer.InstallationCluster;

/**
 * Running the install method for every service in the services array using the cluster info.
 */
public class ServicesInstaller extends SystemObjectImpl {
	public InstallationCluster cluster;
	public Service[] services;

	/**
	 * invoke install method for all services in services array.
	 * 
	 * @throws Exception
	 */
	public void installServices() throws Exception {
		for (int i = 0; i < services.length; i++) {
			services[i].install(cluster);
		}
	}

	public Service[] getServices() {
		return services;
	}

	public void setServices(Service[] services) {
		this.services = services;
	}

	public InstallationCluster getCluster() {
		return cluster;
	}

	public void setCluster(InstallationCluster cluster) {
		this.cluster = cluster;
	}
}