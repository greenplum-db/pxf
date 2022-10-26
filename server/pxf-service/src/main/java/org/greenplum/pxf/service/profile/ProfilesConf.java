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


import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.model.PluginConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static org.greenplum.pxf.service.profile.ProfileConfException.MessageFormat.NO_PLUGINS_IN_PROFILE_DEF;
import static org.greenplum.pxf.service.profile.ProfileConfException.MessageFormat.NO_PROFILE_DEF;
import static org.greenplum.pxf.service.profile.ProfileConfException.MessageFormat.PROFILES_FILE_LOAD_ERR;
import static org.greenplum.pxf.service.profile.ProfileConfException.MessageFormat.PROFILES_FILE_NOT_FOUND;

/**
 * This class holds the profiles files: pxf-profiles.xml and pxf-profiles-default.xml.
 * It exposes a public static method getProfilePluginsMap(String plugin) which returns the requested profile plugins
 */
@Component
public class ProfilesConf implements PluginConf {
    private final static String EXTERNAL_PROFILES = "pxf-profiles.xml";
    private final static String INTERNAL_PROFILES = "pxf-profiles-default.xml";

    private final static Logger LOG = LoggerFactory.getLogger(ProfilesConf.class);
    private final String externalProfilesFilename;

    // maps a profileName --> Profile object
    private Map<String, Profile> profilesMap;
    private Pattern dynamicProfilePattern;

    /**
     * Constructs the ProfilesConf enum singleton instance.
     * <p/>
     * External profiles take precedence over the internal ones and override them.
     */
    @Autowired
    public ProfilesConf( @Value( "${pxf.profile.dynamic.regex}" ) String dynamicProfilesRegex) {
        this(INTERNAL_PROFILES, EXTERNAL_PROFILES, dynamicProfilesRegex);
    }

    ProfilesConf(String internalProfilesFilename, String externalProfilesFilename, String dynamicProfilesRegex) {
        this.profilesMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.externalProfilesFilename = externalProfilesFilename;

        loadConf(internalProfilesFilename, true);
        loadConf(externalProfilesFilename, false);
        if (profilesMap.isEmpty()) {
            throw new ProfileConfException(PROFILES_FILE_NOT_FOUND, externalProfilesFilename);
        }
        if (StringUtils.isNotBlank(dynamicProfilesRegex)) {
            dynamicProfilePattern = Pattern.compile(dynamicProfilesRegex);
            LOG.info("PXF profiles dynamic regex: {}", dynamicProfilesRegex);
        }
        LOG.info("PXF profiles loaded: {}", profilesMap.keySet());
    }

    @Override
    public Map<String, String> getOptionMappings(String profileName) {
        Profile profile = getProfile(profileName);
        return profile != null ? profile.getOptionsMap() : null;
    }

    /**
     * Get requested profile plugins map.
     * In case pxf-profiles.xml is not on the classpath, or it doesn't contains the requested profile,
     * Fallback to pxf-profiles-default.xml occurs (@see useProfilesDefaults(String msgFormat))
     *
     * @param profileName The requested profile
     * @return Plugins map of the requested profile
     */
    @Override
    public Map<String, String> getPlugins(String profileName) {
        Profile profile = getProfile(profileName);
        if (profile == null) {
            return null;
        }
        Map<String, String> result = profile.getPluginsMap();
        if (result.isEmpty()) {
            throw new ProfileConfException(NO_PLUGINS_IN_PROFILE_DEF, profileName, externalProfilesFilename);
        }
        return result;
    }

    @Override
    public String getProtocol(String profileName) {
        Profile profile = getProfile(profileName);
        return profile != null ? profile.getProtocol() : null;
    }

    @Override
    public String getHandler(String profileName) {
        Profile profile = getProfile(profileName);
        return profile != null ? profile.getHandler() : null;
    }

    private Profile getProfile(String profileName) {
        Profile profile = profilesMap.get(profileName);
        if (profile == null && !isDynamicProfile(profileName)) {
            throw new ProfileConfException(NO_PROFILE_DEF, profileName, externalProfilesFilename);
        }
        return profile;
    }

    private boolean isDynamicProfile(String profileName) {
        return dynamicProfilePattern != null && dynamicProfilePattern.matcher(profileName).matches();
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
                LOG.info("Profile file '{}' is empty", fileName);
                return;
            }

            Set<String> processedProfiles = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            for (Profile profile : profiles.getProfiles()) {
                String profileName = profile.getName();

                if (processedProfiles.contains(profileName)) {
                    LOG.warn("Duplicate profile definition found in '{}' for '{}'", fileName, profileName);
                    continue;
                }

                processedProfiles.add(profileName);
                // update internal map with the new profile definitions
                profilesMap.put(profileName, profile);

                List<Profile.Mapping> mappings = profile.getMappingList();
                Map<String, String> optionsMap = profile.getOptionsMap();

                // We were unable to get this working in the Profile class
                if (mappings != null && mappings.size() > 0) {
                    mappings.forEach(m -> optionsMap.put(m.getOption(), m.getProperty()));
                }
            }
            LOG.info("Processed {} profiles from file {}", processedProfiles.size(), fileName);

        } catch (JAXBException e) {
            throw new ProfileConfException(PROFILES_FILE_LOAD_ERR, url.getFile(), String.valueOf(e.getCause()));
        }
    }

    private ClassLoader getClassLoader() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        return (classLoader != null)
                ? classLoader
                : ProfilesConf.class.getClassLoader();
    }
}
