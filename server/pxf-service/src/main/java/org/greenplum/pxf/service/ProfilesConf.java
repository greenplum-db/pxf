package org.greenplum.pxf.service;

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


import org.greenplum.pxf.api.model.PluginConf;
import org.greenplum.pxf.api.model.Profile;
import org.greenplum.pxf.api.model.Profiles;
import org.greenplum.pxf.api.utilities.ProfileConfException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.net.URL;
import java.util.Map;
import java.util.TreeMap;

import static org.greenplum.pxf.api.utilities.ProfileConfException.MessageFormat.NO_PLUGINS_IN_PROFILE_DEF;
import static org.greenplum.pxf.api.utilities.ProfileConfException.MessageFormat.NO_PROFILE_DEF;
import static org.greenplum.pxf.api.utilities.ProfileConfException.MessageFormat.PROFILES_FILE_LOAD_ERR;
import static org.greenplum.pxf.api.utilities.ProfileConfException.MessageFormat.PROFILES_FILE_NOT_FOUND;

/**
 * This class holds the profiles files: pxf-profiles.xml and pxf-profiles-default.xml.
 * It exposes a public static method getProfilePluginsMap(String plugin) which returns the requested profile plugins
 */
public class ProfilesConf implements PluginConf {
    private final static String EXTERNAL_PROFILES = "pxf-profiles.xml";
    private final static String INTERNAL_PROFILES = "pxf-profiles-default.xml";

    private final static Logger LOG = LoggerFactory.getLogger(ProfilesConf.class);
    private final static ProfilesConf INSTANCE = new ProfilesConf();
    private final String externalProfilesFilename;
    private Map<String, Profile> profilesMap;

    /**
     * Constructs the ProfilesConf enum singleton instance.
     * <p/>
     * External profiles take precedence over the internal ones and override them.
     */
    private ProfilesConf() {
        this(INTERNAL_PROFILES, EXTERNAL_PROFILES);
    }

    ProfilesConf(String internalProfilesFilename, String externalProfilesFilename) {
        this.profilesMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.externalProfilesFilename = externalProfilesFilename;

        loadConf(internalProfilesFilename, true);
        loadConf(externalProfilesFilename, false);
        if (profilesMap.isEmpty()) {
            throw new ProfileConfException(PROFILES_FILE_NOT_FOUND, externalProfilesFilename);
        }
        LOG.info("PXF profiles loaded: {}", profilesMap.keySet());
    }

    public static ProfilesConf getInstance() {
        return INSTANCE;
    }


    @Override
    public Map<String, String> getOptionMappings(String key) {
        return getProfile(key).getOptionMappingTable();
    }

    /**
     * Get requested profile plugins map.
     * In case pxf-profiles.xml is not on the classpath, or it doesn't contains the requested profile,
     * Fallback to pxf-profiles-default.xml occurs (@see useProfilesDefaults(String msgFormat))
     *
     * @param key The requested profile
     * @return Plugins map of the requested profile
     */
    @Override
    public Map<String, String> getPlugins(String key) {
        Profile profile = getProfile(key);
        Map<String, String> pluginsMap = profile.getPluginTable();
        if (pluginsMap == null) {
            throw new ProfileConfException(NO_PLUGINS_IN_PROFILE_DEF, key, externalProfilesFilename);
        }
        return pluginsMap;
    }

    @Override
    public String getProtocol(String key) {
        return getProfile(key).getProtocol();
    }

    private Profile getProfile(String key) {
        Profile profile = profilesMap.get(key);
        if (profile == null) {
            throw new ProfileConfException(NO_PROFILE_DEF, key, externalProfilesFilename);
        }
        return profile;
    }

    private ClassLoader getClassLoader() {
        ClassLoader cL = Thread.currentThread().getContextClassLoader();
        return (cL != null)
                ? cL
                : ProfilesConf.class.getClassLoader();
    }

    private void loadConf(String fileName, boolean isMandatory) {
        URL url = getClassLoader().getResource(fileName);
        if (url == null) {
            LOG.warn("{} not found in the classpath", fileName);
            if (isMandatory) {
                throw new ProfileConfException(PROFILES_FILE_NOT_FOUND, fileName);
            }
            return;
        }
        try {
            JAXBContext jc = JAXBContext.newInstance(Profiles.class);
            Unmarshaller unmarshaller = jc.createUnmarshaller();
            Profiles profiles = (Profiles) unmarshaller.unmarshal(url);

            if (profiles == null || profiles.getProfiles() == null || profiles.getProfiles().isEmpty()) {
                LOG.warn("Profile file: {} is empty", fileName);
                return;
            }

            Map<String, Profile> profileMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            for (Profile profile : profiles.getProfiles()) {
                String profileName = profile.getName();

                if (profileMap.containsKey(profileName)) {
                    LOG.warn("Duplicate profile definition found in {} for: {}", fileName, profileName);
                    continue;
                }

                profileMap.put(profileName, profile);
            }

            profilesMap.putAll(profileMap);

        } catch (JAXBException e) {
            throw new ProfileConfException(PROFILES_FILE_LOAD_ERR, url.getFile(), String.valueOf(e.getCause()));
        }
    }
}
