package org.greenplum.pxf.automation.features.profiles;

import jsystem.framework.system.SystemManagerImpl;
import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.enums.EnumPxfDefaultProfiles;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.profiles.Profile;
import org.greenplum.pxf.automation.structures.profiles.PxfProfileXml;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.exception.ExceptionUtils;
import org.postgresql.util.PSQLException;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests Package for Profiles feature in PXF
 */
public class ProfilesTest extends BaseFeature {

    private PxfProfileXml pxfProfiles;

    private boolean requiresRestartAfterTest;

    /**
     * Runs always before test method
     *
     * @throws Exception if test fails to run
     */
    @Override
    protected void beforeMethod() throws Exception {

        requiresRestartAfterTest = false;

        super.beforeMethod();
        pxfProfiles = cluster.getPxfProfiles();
        exTable = TableFactory.getPxfReadableTextTable("pxf_profiles_small_data", new String[]{
                "name text",
                "num integer",
                "dub double precision",
                "longNum bigint",
                "bool boolean"
        }, hdfs.getWorkingDirectory() + "/" + fileName, ",");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
    }

    /**
     * Runs always after test Method
     *
     * @throws Exception if test fails to run
     */
    @Override
    protected void afterMethod() throws Exception {

        super.afterMethod();

        if (requiresRestartAfterTest) {
            pxfProfiles.restore();
            cluster.copyFileToNodes(pxfProfiles.getXmlFilePath(), cluster.getPxfConfLocation(),
                    false, false);
            cluster.restart(PhdCluster.EnumClusterServices.pxf);
            exTable.setUserParameters(null);
        }
    }

    @Override
    protected void beforeClass() throws Exception {

        // get cluster from sut file
        cluster = (PhdCluster) SystemManagerImpl.getInstance().getSystemObject("cluster");
        // Generate small data file
        Table dataTable = getSmallData();
        // Upload file to HDFS
        hdfs.writeTableToFile(hdfs.getWorkingDirectory() + "/" + fileName, dataTable, ",");
    }

    /**
     * check basic HdfsTextSimple profile with no change is working
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"sanity"})
    protected void sanity() throws Exception {

        gpdb.createTableAndVerify(exTable);
        runVerificationSql();
    }

    /**
     * Go over profiles XML and check all required profiles exists.
     * Currently disabled until HiveORC profile is complete.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = "unused", enabled = false)
    public void defaultProfilesExists() throws Exception {

        // go over all profiles and verify required profiles exists
        for (EnumPxfDefaultProfiles profile : EnumPxfDefaultProfiles.values()) {
            if (pxfProfiles.getProfile(profile.toString()) == null) {
                throw new Exception("Profile not found in pxf-profiles.xml: " + profile);
            }
        }
    }

    /**
     * Check profile name is insensitive
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = "unused")
    public void caseInsensitive() throws Exception {

        exTable.setProfile(EnumPxfDefaultProfiles.HdfsTextSimple.toString().toUpperCase());
        gpdb.createTableAndVerify(exTable);
        runVerificationSql();
    }

    /**
     * put profile parameter last in location
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"profile"})
    public void profileParameterNotAsFirstParameter() throws Exception {

        exTable.setProfile(null);
        exTable.setUserParameters(new String[]{"Ready=Go", "Profile=" +
                EnumPxfDefaultProfiles.HdfsTextSimple.toString()});
        gpdb.createTableAndVerify(exTable);
        runVerificationSql();
    }

    /**
     * create new profile based on HdfsTestSimple
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"profile"})
    public void customProfile() throws Exception {

        requiresRestartAfterTest = true;

        // get "HdfsTextSimple" profile and change the name
        Profile hdfsTextSimpleProfile = new Profile("BeHereNow");
        hdfsTextSimpleProfile.setDescription("BeHereNow");
        hdfsTextSimpleProfile.setAccessor("org.greenplum.pxf.plugins.hdfs.LineBreakAccessor");
        hdfsTextSimpleProfile.setResolver("org.greenplum.pxf.plugins.hdfs.StringPassResolver");
        hdfsTextSimpleProfile.setFragmenter("org.greenplum.pxf.plugins.hdfs.HdfsDataFragmenter");
        // clean profiles list
        pxfProfiles.initProfilesList();
        // add "edited" profile to list
        pxfProfiles.addProfile(hdfsTextSimpleProfile);
        // write list to file
        pxfProfiles.writeProfilesListToFile();

        cluster.copyFileToNodes(pxfProfiles.getXmlFilePath(), cluster.getPxfBase() + "/conf");
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
        exTable.setProfile("BeHereNow");
        gpdb.createTableAndVerify(exTable);
        runVerificationSql();
    }

    /**
     * work without profile in location
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"profile"})
    public void noProfile() throws Exception {

        exTable.setProfile(null);
        exTable.setFragmenter("org.greenplum.pxf.plugins.hdfs.HdfsDataFragmenter");
        exTable.setAccessor("org.greenplum.pxf.plugins.hdfs.LineBreakAccessor");
        exTable.setResolver("org.greenplum.pxf.plugins.hdfs.StringPassResolver");
        gpdb.createTableAndVerify(exTable);

        runVerificationSql();

        exTable.setFragmenter(null);
        exTable.setAccessor(null);
        exTable.setResolver(null);
    }

    /**
     * work with same profile defined multiple times
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = {"profile"})
    public void duplicateProfile() throws Exception {

        exTable.setProfile(null);
        exTable.setUserParameters(new String[]{
                "Profile=" + EnumPxfDefaultProfiles.HdfsTextSimple.toString().toUpperCase(),
                "Profile=" + EnumPxfDefaultProfiles.HdfsTextSimple
        });

        try {
            gpdb.createTableAndVerify(exTable);
            Assert.fail("Exception should have been thrown because of duplicate profile");
        } catch (PSQLException e) {
            ExceptionUtils.validate(null, e,
                    new PSQLException("ERROR: .?nvalid URI pxf://" + exTable.getPath() +
                            "\\?Profile=HDFSTEXTSIMPLE&Profile=HdfsTextSimple: " +
                            "Duplicate option\\(s\\): PROFILE", null), true);
        }
    }

    private void runVerificationSql() throws Exception {

        runSqlTest("features/profiles/small_data");
    }
}
