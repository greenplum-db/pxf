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

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.greenplum.pxf.service.profile.ProfileConfException.MessageFormat.NO_PLUGINS_IN_PROFILE_DEF;
import static org.greenplum.pxf.service.profile.ProfileConfException.MessageFormat.NO_PROFILE_DEF;
import static org.greenplum.pxf.service.profile.ProfileConfException.MessageFormat.PROFILES_FILE_LOAD_ERR;
import static org.greenplum.pxf.service.profile.ProfileConfException.MessageFormat.PROFILES_FILE_NOT_FOUND;

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

    // index maps of (profileName --> protocol)
    private Map<String, String> protocolMap;

    // index map of (profileName --> (pluginType --> pluginClass))
    private Map<String, Map<String, String>> pluginsMap;

    // index map of (profileName --> (optionName --> propertyName))
    private Map<String, Map<String, String>> configOptionsMap;

    /**
     * Constructs the ProfilesConf enum singleton instance.
     * <p/>
     * External profiles take precedence over the internal ones and override them.
     */
    private ProfilesConf() {
        this(INTERNAL_PROFILES, EXTERNAL_PROFILES);
    }

    ProfilesConf(String internalProfilesFilename, String externalProfilesFilename) {
        this.protocolMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.pluginsMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.configOptionsMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.externalProfilesFilename = externalProfilesFilename;

        loadConf(internalProfilesFilename, true);
        loadConf(externalProfilesFilename, false);
        if (pluginsMap.isEmpty()) {
            throw new ProfileConfException(PROFILES_FILE_NOT_FOUND, externalProfilesFilename);
        }
        LOG.info("PXF profiles loaded: {}", pluginsMap.keySet());
    }

    public static ProfilesConf getInstance() {
        return INSTANCE;
    }

    @Override
    public Map<String, String> getOptionMappings(String key) {
        return configOptionsMap.get(key);
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
        Map<String, String> result = pluginsMap.get(profileName);
        if (result == null) {
            throw new ProfileConfException(NO_PROFILE_DEF, profileName, externalProfilesFilename);
        }
        if (result.isEmpty()) {
            throw new ProfileConfException(NO_PLUGINS_IN_PROFILE_DEF, profileName, externalProfilesFilename);
        }
        return result;
    }

    @Override
    public String getProtocol(String profileName) {
        return protocolMap.get(profileName);
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
                LOG.warn("Profile file '{}' is empty", fileName);
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
                // update internal maps with the new profile definitions
                pluginsMap.put(profileName, getPluginsForProfile(profile));
                protocolMap.put(profileName, profile.getProtocol());
                configOptionsMap.put(profileName, getOptionMappingsForProfile(profile));
            }

        } catch (JAXBException e) {
            throw new ProfileConfException(PROFILES_FILE_LOAD_ERR, url.getFile(), String.valueOf(e.getCause()));
        }
    }

    private Map<String, String> getPluginsForProfile(Profile profile) {

        Map<String, String> profilePluginsMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        Profile.Plugins profilePlugins = profile.getPlugins();
        if (profilePlugins != null) {
            String fragmenter = profilePlugins.getFragmenter();
            if (StringUtils.isNotBlank(fragmenter)) {
                profilePluginsMap.put(Profile.Plugins.FRAGMENTER, fragmenter);
            }

            String accessor = profilePlugins.getAccessor();
            if (StringUtils.isNotBlank(accessor)) {
                profilePluginsMap.put(Profile.Plugins.ACCESSOR, accessor);
            }

            String resolver = profilePlugins.getResolver();
            if (StringUtils.isNotBlank(resolver)) {
                profilePluginsMap.put(Profile.Plugins.RESOLVER, resolver);
            }

            String metadata = profilePlugins.getMetadata();
            if (StringUtils.isNotBlank(metadata)) {
                profilePluginsMap.put(Profile.Plugins.METADATA, metadata);
            }

            String outputFormat = profilePlugins.getOutputFormat();
            if (StringUtils.isNotBlank(outputFormat)) {
                profilePluginsMap.put(Profile.Plugins.OUTPUTFORMAT, outputFormat);
            }
        }
        return profilePluginsMap;
    }

    private Map<String, String> getOptionMappingsForProfile(Profile profile) {

        Map<String, String> profileOptionMappingsMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        List<Profile.Mapping> mappingList = profile.getOptionMappings();
        if (mappingList != null) {
            mappingList.forEach(m->profileOptionMappingsMap.put(m.getOption(), m.getProperty()));
        }
        return profileOptionMappingsMap;
    }

    private ClassLoader getClassLoader() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        return (classLoader != null)
                ? classLoader
                : ProfilesConf.class.getClassLoader();
    }
}
