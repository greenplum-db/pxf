package org.greenplum.pxf.service.profile;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Base test class for all ProfilesConf tests. Each test case is encapsulated
 * inside its own inner class to force reloading of ProfilesConf enum singleton
 */
public class ProfilesConfTest {

    @Test
    public void definedProfile() {
        ProfilesConf profilesConf = getProfilesConf("definedProfile");
        Map<String, String> hbaseProfile = profilesConf.getPlugins("HBase");
        assertEquals(2, hbaseProfile.keySet().size());
        assertEquals("X", hbaseProfile.get("OUTPUTFORMAT"));
        assertEquals("XX", hbaseProfile.get("METADATA"));

        // case insensitive profile name
        Map<String, String> hiveProfile = profilesConf.getPlugins("hIVe");
        assertEquals(1, hiveProfile.keySet().size());
        assertEquals("Y", hiveProfile.get("OUTPUTFORMAT"));
    }

    @Test
    public void undefinedProfile() {
        ProfilesConf profilesConf = getProfilesConf("undefinedProfile");
        Exception e = assertThrows(ProfileConfException.class,
                () -> profilesConf.getPlugins("UndefinedProfile"));
        assertEquals("UndefinedProfile is not defined in profile/undefinedProfile/pxf-profiles.xml", e.getMessage());
    }

    @Test
    public void undefinedDynamicProfile() {
        String profileName = "test:foo";
        ProfilesConf profilesConf = getProfilesConfWithTestDynamicRegex("undefinedProfile");
        assertNull(profilesConf.getHandler(profileName));
        assertNull(profilesConf.getOptionMappings(profileName));
        assertNull(profilesConf.getPlugins(profileName));
        assertNull(profilesConf.getProtocol(profileName));
    }

    @Test
    public void testUndefinedProfileWhenGettingProtocol() {
        ProfilesConf profilesConf = getProfilesConf("undefinedProfile");

        Exception e = assertThrows(ProfileConfException.class,
                () -> profilesConf.getProtocol("UndefinedProfile"));
        assertEquals("UndefinedProfile is not defined in profile/undefinedProfile/pxf-profiles.xml", e.getMessage());
    }

    @Test
    public void testGetProtocol() {
        ProfilesConf profilesConf = getProfilesConf("testGetProtocol");
        assertEquals("bar", profilesConf.getProtocol("foo"));
    }

    @Test
    public void testGetHandler() {
        ProfilesConf profilesConf = getProfilesConf("testGetProtocol");
        assertEquals("FooProtocolHandler.class", profilesConf.getHandler("with-handler"));
        assertNull(profilesConf.getHandler("foo"));
    }

    @Test
    public void testGetMissingProtocol() {
        ProfilesConf profilesConf = getProfilesConf("testGetProtocol");
        assertNull(profilesConf.getProtocol("bar"));
    }

    @Test
    public void duplicateProfileDefinition() {
        ProfilesConf profilesConf = getProfilesConf("duplicateProfileDefinition");
        profilesConf.getPlugins("HBase");
    }

    @Test
    public void overrideProfile() {
        ProfilesConf profilesConf = getProfilesConf("overrideProfile");
        Map<String, String> profile = profilesConf.getPlugins("HBase");
        assertEquals(2, profile.keySet().size());
        assertEquals("Y", profile.get("ACCESSOR"));
        assertEquals("YY", profile.get("RESOLVER"));
    }

    @Test
    public void emptyProfileFile() {
        ProfilesConf profilesConf = getProfilesConf("emptyProfileFile");
        profilesConf.getPlugins("HBase");
    }

    @Test
    public void emptyProfilePluginsFile() {
        ProfilesConf profilesConf = getProfilesConf("emptyProfilePluginsFile");
        Exception e = assertThrows(ProfileConfException.class,
                () -> profilesConf.getPlugins("hdfs:text"));
        assertEquals("Profile hdfs:text does not define any plugins in profile/emptyProfilePluginsFile/pxf-profiles.xml", e.getMessage());
    }

    @Test
    public void malformedProfileFile() {
        Exception e = assertThrows(ProfileConfException.class,
                () -> getProfilesConf("malformedProfileFile"));
        assertTrue(e.getMessage().contains("pxf-profiles-default.xml could not be loaded: org.xml.sax.SAXParseException"));
    }

    @Test
    public void missingMandatoryProfileFile() {
        Exception e = assertThrows(ProfileConfException.class,
                () -> getProfilesConf("missingMandatoryProfileFile"));
        assertEquals("profile/missingMandatoryProfileFile/pxf-profiles-default.xml was not found in the CLASSPATH", e.getMessage());
    }

    @Test
    public void missingOptionalProfileFile() {
        ProfilesConf profilesConf = getProfilesConf("missingOptionalProfileFile");

        Map<String, String> hbaseProfile = profilesConf.getPlugins("HBase");
        assertEquals("Y", hbaseProfile.get("FRAGMENTER"));
    }

    @Test
    public void testOptionMappingsMissingMapping() {
        ProfilesConf profilesConf = getProfilesConf("optionMappings");
        Map<String, String> map = profilesConf.getOptionMappings("missing-mappings");
        assertTrue(map.isEmpty());
    }

    @Test
    public void testOptionMappingsEmptyMapping() {
        ProfilesConf profilesConf = getProfilesConf("optionMappings");
        Map<String, String> map = profilesConf.getOptionMappings("empty-mappings");
        assertTrue(map.isEmpty());
    }

    @Test
    public void testOptionMappingsOneMapping() {
        ProfilesConf profilesConf = getProfilesConf("optionMappings");
        Map<String, String> map = profilesConf.getOptionMappings("one-mapping");

        assertEquals(1, map.size());
        assertEquals("property1", map.get("option1"));
    }

    @Test
    public void testOptionMappingsTwoMappings() {
        ProfilesConf profilesConf = getProfilesConf("optionMappings");
        Map<String, String> map = profilesConf.getOptionMappings("two-mappings");

        assertEquals(2, map.size());
        assertEquals("prop1", map.get("option1"));
        assertEquals("prop2", map.get("option2"));
    }

    @Test
    public void testProfileWithSpacesInName() {
        ProfilesConf profilesConf = getProfilesConf("profileWithSpacesInName");

        Map<String, String> map = profilesConf.getPlugins("HBase");
        assertNotNull(map);
        assertEquals(1, map.size());
        assertEquals("Y", map.get("accessor"));
    }

    @Test
    public void testMalformedXmlFile() {
        Exception e = assertThrows(ProfileConfException.class,
                () -> getProfilesConf("malformedXmlFile"));
        assertThat(e.getMessage(), matchesPattern("^Profiles configuration .+ could not be loaded:.*"));
    }

    @Test
    public void missingPluginFile() {
        ProfilesConf profilesConf = getProfilesConf("missingPluginFile");
        profilesConf.getPlugins("hdfs:text");
    }

    private ProfilesConf getProfilesConf(String testCase) {
        return new ProfilesConf(String.format("profile/%s/pxf-profiles-default.xml", testCase),
                String.format("profile/%s/pxf-profiles.xml", testCase), null);
    }

    private ProfilesConf getProfilesConfWithTestDynamicRegex(String testCase) {
        return new ProfilesConf(String.format("profile/%s/pxf-profiles-default.xml", testCase),
                String.format("profile/%s/pxf-profiles.xml", testCase), "test:.*");
    }

}
