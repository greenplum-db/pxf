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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Base test class for all ProfilesConf tests. Each test case is encapsulated
 * inside its own inner class to force reloading of ProfilesConf enum singleton
 */
public class ProfilesConfTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

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
        expectedException.expect(ProfileConfException.class);
        expectedException.expectMessage("UndefinedProfile is not defined in profile/undefinedProfile/pxf-profiles.xml");

        ProfilesConf profilesConf = getProfilesConf("undefinedProfile");
        profilesConf.getPlugins("UndefinedProfile");
    }

    @Test
    public void testUndefinedProfileWhenGettingProtocol() {
        ProfilesConf profilesConf = getProfilesConf("undefinedProfile");
        assertNull(profilesConf.getProtocol("UndefinedProfile"));
    }

    @Test
    public void testGetProtocol() {
        ProfilesConf profilesConf = getProfilesConf("testGetProtocol");
        assertEquals("bar", profilesConf.getProtocol("foo"));
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
        Map profile = profilesConf.getPlugins("HBase");
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
    public void malformedProfileFile() {
        expectedException.expect(ProfileConfException.class);
        expectedException.expectMessage("pxf-profiles-default.xml could not be loaded: org.xml.sax.SAXParseException");

        ProfilesConf profilesConf = getProfilesConf("malformedProfileFile");
        profilesConf.getPlugins("HBase");
    }

    @Test
    public void missingMandatoryProfileFile() {
        expectedException.expect(ProfileConfException.class);
        expectedException.expectMessage("profile/missingMandatoryProfileFile/pxf-profiles-default.xml was not found in the CLASSPATH");

        ProfilesConf profilesConf = getProfilesConf("missingMandatoryProfileFile");
        profilesConf.getPlugins("HBase");
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

    private ProfilesConf getProfilesConf(String testCase) {
        return new ProfilesConf(String.format("profile/%s/pxf-profiles-default.xml", testCase),
                String.format("profile/%s/pxf-profiles.xml", testCase));
    }

}
