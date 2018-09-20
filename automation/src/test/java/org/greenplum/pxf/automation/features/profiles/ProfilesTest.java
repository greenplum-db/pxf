package org.greenplum.pxf.automation.features.profiles;

import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.enums.EnumPxfDefaultProfiles;
import org.greenplum.pxf.automation.structures.profiles.Profile;
import org.greenplum.pxf.automation.structures.profiles.PxfProfileXml;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.utils.TableFactory;
import org.greenplum.pxf.automation.utils.exception.ExceptionUtils;
import org.greenplum.pxf.automation.utils.system.PGModeEnum;
import org.greenplum.pxf.automation.utils.system.SystemUtils;
import org.greenplum.pxf.automation.features.BaseFeature;
import jsystem.framework.system.SystemManagerImpl;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.postgresql.util.PSQLException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;

/** Tests Package for Profiles feature in PXF */
public class ProfilesTest extends BaseFeature {

    private PxfProfileXml pxfProfiles;

    /**
     * Runs always before test method
     *
     * @throws Exception if test fails to run
     */
    @Override
    protected void beforeMethod() throws Exception {

        super.beforeMethod();
        pxfProfiles = cluster.getPxfProfiles();
        exTable = TableFactory.getPxfReadableTextTable("pxf_profiles_small_data", new String[] {
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
        pxfProfiles.restore();
        cluster.copyFileToNodes(pxfProfiles.getXmlFilePath(), cluster.getPxfConfLocation(),
                false, false);
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
        exTable.setUserParameters(null);
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
    @Test(groups = { "features", "sanity" })
    protected void sanity() throws Exception {

        gpdb.createTableAndVerify(exTable);
        runVerificationTinc();
    }

    /**
     * Go over profiles XML and check all required profiles exists.
     * Currently disabled until HiveORC profile is complete.
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = "features", enabled = false)
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
    @Test(groups = "features")
    public void caseInsensetive() throws Exception {

        exTable.setProfile(EnumPxfDefaultProfiles.HdfsTextSimple.toString().toUpperCase());
        gpdb.createTableAndVerify(exTable);
        runVerificationTinc();
    }

    /**
     * put profile parameter last in location
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "features", "gpdb" })
    public void profileParameterNotAsFirstParameter() throws Exception {

        exTable.setProfile(null);
        exTable.setUserParameters(new String[] { "Ready=Go", "Profile=" +
                EnumPxfDefaultProfiles.HdfsTextSimple.toString() });
        gpdb.createTableAndVerify(exTable);
        runVerificationTinc();
    }

    /**
     * not exists profile
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "features", "gpdb" })
    public void missingProfile() throws Exception {

        // set not exists profile
        exTable.setProfile(EnumPxfDefaultProfiles.HdfsTextSimple.toString() + "_Fake");
        gpdb.createTableAndVerify(exTable);
        runTincTest("pxf.features.profiles.errors.missingProfile.runTest");
    }

    /**
     * empty profile in profile XML. should work with default XML in jar
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "features", "gpdb" })
    public void emptyProfile() throws Exception {

        // clean profiles list, add empty profile and write to file
        pxfProfiles.initProfilesList();
        pxfProfiles.addProfile(new Profile("HdfsTextSimple"));
        pxfProfiles.writeProfilesListToFile();

        cluster.copyFileToNodes(pxfProfiles.getXmlFilePath(), cluster.getPxfConfLocation());
        cluster.restart(PhdCluster.EnumClusterServices.pxf);

        exTable.setProfile(EnumPxfDefaultProfiles.HdfsTextSimple.toString());
        gpdb.createTableAndVerify(exTable);
        runTincTest("pxf.features.profiles.errors.emptyProfile.runTest");
    }

    /**
     * profile name in XML with spaces, should work
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "features", "gpdb" })
    public void profileNameWithSpaces() throws Exception {

        // get "HdfsTextSimple" profile, edit name to be wrapped with spaces and write to file
        Profile hdfsTextSimpleProfile = pxfProfiles.getProfile(EnumPxfDefaultProfiles.HdfsTextSimple.toString());
        hdfsTextSimpleProfile.setName("     " + EnumPxfDefaultProfiles.HdfsTextSimple.toString() + "     ");
        pxfProfiles.writeProfilesListToFile();
        cluster.copyFileToNodes(pxfProfiles.getXmlFilePath(), cluster.getPxfConfLocation());
        cluster.restart(PhdCluster.EnumClusterServices.pxf);

        exTable.setProfile(EnumPxfDefaultProfiles.HdfsTextSimple.toString());
        gpdb.createTableAndVerify(exTable);
        runVerificationTinc();
    }

    /**
     * missing fragmenter to profile should throw Error
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "features", "gpdb" })
    public void missingPlugin() throws Exception {

        // get "HdfsTextSimple" profile, remove fragmenter and write to file
        Profile hdfsTextSimpleProfile = pxfProfiles.getProfile(EnumPxfDefaultProfiles.HdfsTextSimple.toString());
        hdfsTextSimpleProfile.setFragmenter(null);
        pxfProfiles.writeProfilesListToFile();

        cluster.copyFileToNodes(pxfProfiles.getXmlFilePath(), cluster.getPxfConfLocation());
        cluster.restart(PhdCluster.EnumClusterServices.pxf);

        exTable.setProfile(EnumPxfDefaultProfiles.HdfsTextSimple.toString());
        gpdb.createTableAndVerify(exTable);
        runTincTest("pxf.features.profiles.errors.missingPlugin.runTest");
    }

    /**
     * create new profile based on HdfsTestSimple
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "features", "gpdb" })
    public void customProfile() throws Exception {

        // get "HdfsTextSimple" profile and change the name
        Profile hdfsTextSimpleProfile = pxfProfiles.getProfile(EnumPxfDefaultProfiles.HdfsTextSimple.toString());
        hdfsTextSimpleProfile.setName("BeHereNow");
        // clean profiles list
        pxfProfiles.initProfilesList();
        // add "edited" profile to list
        pxfProfiles.addProfile(hdfsTextSimpleProfile);
        // write list to file
        pxfProfiles.writeProfilesListToFile();

        cluster.copyFileToNodes(pxfProfiles.getXmlFilePath(), cluster.getPxfConfLocation());
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
        exTable.setProfile("BeHereNow");
        gpdb.createTableAndVerify(exTable);
        runVerificationTinc();
    }

    /**
     * work without profile in location
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "features", "gpdb" })
    public void noProfile() throws Exception {

        exTable.setProfile(null);
        exTable.setFragmenter("org.greenplum.pxf.plugins.hdfs.HdfsDataFragmenter");
        exTable.setAccessor("org.greenplum.pxf.plugins.hdfs.LineBreakAccessor");
        exTable.setResolver("org.greenplum.pxf.plugins.hdfs.StringPassResolver");
        gpdb.createTableAndVerify(exTable);

        runVerificationTinc();

        exTable.setFragmenter(null);
        exTable.setAccessor(null);
        exTable.setResolver(null);
    }

    /**
     * Error using malformed XML
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "features", "gpdb" })
    public void malformedXmlFile() throws Exception {

        File xmlProfileFile = new File(pxfProfiles.getXmlFilePath());
        String xmlAsString = FileUtils.readFileToString(xmlProfileFile);
        // remove all opening tags and write to file
        FileUtils.writeStringToFile(xmlProfileFile, xmlAsString.replaceAll("<", ""));

        cluster.copyFileToNodes(xmlProfileFile.getAbsolutePath(), cluster.getPxfConfLocation());
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
        // wait for change to load
        Thread.sleep(2000);
        gpdb.createTableAndVerify(exTable);
        runTincTest("pxf.features.profiles.errors.malformedXmlFile.runTest");
    }

    /**
     * work with same profile defined multiple times
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "features", "gpdb" })
    public void duplicateProfile() throws Exception {

        exTable.setProfile(null);
        exTable.setUserParameters(new String[] {
                "Profile=" + EnumPxfDefaultProfiles.HdfsTextSimple.toString().toUpperCase(),
                "Profile=" + EnumPxfDefaultProfiles.HdfsTextSimple
        });

        try {
            gpdb.createTableAndVerify(exTable);
            Assert.fail("Exception should have been thrown because of duplicate profile");
        } catch (PSQLException e) {
            String address = exTable.getHost();
            if (!StringUtils.isEmpty(exTable.getPort())) {
                address += ":" + exTable.getPort();
            }
            if (SystemUtils.getPGMode() == PGModeEnum.HAWQ) {
				address += "/";
            }
            else {
				address = "";
            }
            ExceptionUtils.validate(null, e,
                    new PSQLException("ERROR: Invalid URI pxf://" + address +
                            exTable.getPath() +
                            "\\?Profile=HDFSTEXTSIMPLE&Profile=HdfsTextSimple: " +
                            "Duplicate option\\(s\\): PROFILE", null), true);
        }
    }

    /**
     * profile has parameters and command line also defines same parameters
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "features", "gpdb" })
    public void duplicateProfileParametersCheck() throws Exception {

        exTable.setProfile(null);
        exTable.setFragmenter("org.greenplum.pxf.plugins.hdfs.HdfsDataFragmenter");
        exTable.setAccessor("org.greenplum.pxf.plugins.hdfs.LineBreakAccessor");
        exTable.setResolver("org.greenplum.pxf.plugins.hdfs.StringPassResolver");
        exTable.setUserParameters(new String[] { "Profile=" + EnumPxfDefaultProfiles.HdfsTextSimple });

        gpdb.createTableAndVerify(exTable);

        runTincTest("pxf.features.profiles.errors.duplicateProfileParametersCheck.runTest");
        exTable.setFragmenter(null);
        exTable.setAccessor(null);
        exTable.setResolver(null);
    }

    /**
     * work with profile and command line parameters as well
     *
     * @throws Exception if test fails to run
     */
    @Test(groups = { "features", "gpdb" })
    public void additionalParameterWithProfile() throws Exception {

        exTable.setProfile(null);
        exTable.setProfile(EnumPxfDefaultProfiles.HdfsTextSimple.toString());
        exTable.setUserParameters(new String[] { "COMPRESSION_CODEC=org.apache.hadoop.io.compress.BZip2Codec" });

        gpdb.createTableAndVerify(exTable);
        runVerificationTinc();
    }

    private void runVerificationTinc() throws Exception {

        runTincTest("pxf.features.profiles.small_data.runTest");
    }
}
