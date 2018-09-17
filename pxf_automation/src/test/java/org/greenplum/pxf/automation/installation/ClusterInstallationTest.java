package com.pxf.automation.installation;

import jsystem.framework.report.ListenerstManager;
import jsystem.framework.system.SystemManagerImpl;

import org.testng.annotations.Test;

import com.pxf.automation.components.cluster.installer.services.ServicesInstaller;

/**
 * Contains one case relates to cluster installation. This case using {@link ServicesInstaller} to
 * install all the services that described in the sut file on the required cluster also described in
 * the sut xml file.
 */
public class ClusterInstallationTest {
    protected ServicesInstaller installer;

    @Test(groups = "installation")
    public void installCluster() throws Exception {
        // turn off jsystem reports
        ListenerstManager.getInstance().setSilent(true);
        // Initialize ServicesInstaller System Object
        installer = (ServicesInstaller) SystemManagerImpl.getInstance().getSystemObject("installer");
        // install all services mentioned in the sut file
        installer.installServices();
    }
}
