package org.greenplum.pxf.automation.structures.profiles;

import java.io.File;
import java.io.IOException;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

import org.greenplum.pxf.automation.utils.files.FileUtils;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;

/**
 * Represents pxf-profiles.xml file.contains {@link List} of {@link Profile} with the possibility to
 * read and write to and from the list.
 * 
 */
public class PxfProfileXml extends XmlBasedComponents {

	public final static String XML_PROFILES_FILE_NAME = "pxf-profiles.xml";

	// List of Profile object
	private List<Profile> profilesList = null;

	/**
	 * C'tor getting profile XML file path and pass it to c'tor that determine whether to already
	 * scan the file into the profiles list or not.
	 * 
	 * @param filePath to profiles XML
	 * @throws Exception
	 */
	public PxfProfileXml(String filePath) throws Exception {
		this(filePath, true);
	}

	/**
	 * Passes a fileName and loads Profiles to profilesList according to scanProfiles parameter
	 * 
	 * @param xmlProfilesFilePath to XML file
	 * @param populateProfilesList if true scan profiles from XML file and populate profilesList
	 * @throws Exception
	 */
	public PxfProfileXml(String xmlProfilesFilePath, boolean populateProfilesList) throws Exception {
		// pass file to parent XmlBasedComponents
		super(xmlProfilesFilePath + "/" + XML_PROFILES_FILE_NAME);

		if (populateProfilesList) {
			populateProfilesList();
		}
	}

	/**
	 * Populates Profiles List from XML
	 * 
	 * @throws Exception
	 */
	public void populateProfilesList() throws Exception {

		initProfilesList();

		ReportUtils.report(null, getClass(), "Load " + XML_PROFILES_FILE_NAME + " from " + getXmlFilePath());

		String[] profileNames = getConf().getStringArray("profile.name");

		for (int profileIdx = 0; profileIdx < profileNames.length; profileIdx++) {

			Configuration profileSubset = getConf().subset("profile(" + profileIdx + ").plugins");

			Profile profile = new Profile(profileNames[profileIdx]);
			profile.setFragmenter(profileSubset.getString("fragmenter"));
			profile.setAccessor(profileSubset.getString("accessor"));
			profile.setResolver(profileSubset.getString("resolver"));
			profile.setDescription(profileSubset.getString("description"));

			profilesList.add(profile);
		}
	}

	/** Deletes XML File */
	public void delete() {
		File xmlProfileFile = new File(getXmlFilePath());
		xmlProfileFile.delete();
	}

	/**
	 * Writes Profile List to file
	 * 
	 * @throws ConfigurationException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void writeProfilesListToFile() throws ConfigurationException, IOException,
			InterruptedException {

		XMLConfiguration conf = new XMLConfiguration();

		conf.setRootElementName("profiles");

		if (profilesList == null) {
			throw new NullPointerException("Profiles List is not initialized");
		}

		for (int profileIndex = 0; profileIndex < profilesList.size(); profileIndex++) {

			Profile profile = profilesList.get(profileIndex);

			conf.addProperty("profile(" + profileIndex + ").name", profile.getName());

			if (profile.getDescription() != null)
				conf.addProperty("profile(" + profileIndex + ").plugins.description", profile.getDescription());

			if (profile.getFragmenter() != null)
				conf.addProperty("profile(" + profileIndex + ").plugins.fragmenter", profile.getFragmenter());

			if (profile.getAccessor() != null)
				conf.addProperty("profile(" + profileIndex + ").plugins.accessor", profile.getAccessor());

			if (profile.getResolver() != null)
				conf.addProperty("profile(" + profileIndex + ").plugins.resolver", profile.getResolver());
		}

		conf.setFileName(getXmlFilePath());
		save(conf);
	}

	/**
	 * Returns the first {@link Profile} according to profileName
	 * 
	 * @param profileName
	 * @return the first profile matching profileName
	 */
	public Profile getProfile(String profileName) {

		for (Profile profile : profilesList) {
			if (profile.getName().equals(profileName)) {
				return profile;
			}
		}

		return null;
	}

	/**
	 * Adds {@link Profile} to the list
	 * 
	 * @param profile
	 */
	public void addProfile(Profile profile) {

		if (profilesList == null) {
			initProfilesList();
		}

		profilesList.add(profile);
	}

	/**
	 * Restores the configuration to file
	 * 
	 * @throws Exception
	 */
	public void restore() throws Exception {
		save(getConf());
		populateProfilesList();
	}

	/**
	 * Sets Permissions for XML profiles file according to set of {@link PosixFilePermission}
	 * 
	 * @param permissions required set of {@link PosixFilePermission}
	 * @throws IOException
	 */
	public void setPermissions(Set<PosixFilePermission> permissions) throws IOException {

		FileUtils.setFilePermission(new File(getXmlFilePath()), permissions);
	}

	/** @return list of {@link Profile} */
	public List<Profile> getProfilesList() {
		return profilesList;
	}

	/** Creates a new empty {@link List} */
	public void initProfilesList() {
		this.profilesList = new ArrayList<Profile>();
	}
}
