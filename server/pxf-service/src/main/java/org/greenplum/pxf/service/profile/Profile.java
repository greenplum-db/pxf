package org.greenplum.pxf.service.profile;

import org.apache.commons.lang.StringUtils;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Represents a PXF {@link Profile}. A profile is a way
 * to simplify access to external data sources. The
 * {@link Profile} consists of {@link Plugins} a protocol
 * and a list of option mappings
 */
@XmlRootElement(name = "profile")
public class Profile {

    @XmlElement(name = "name", required = true)
    private String name;

    @XmlElement(name = "protocol")
    private String protocol;

    @XmlElement(name = "handler")
    private String handler;

    @XmlTransient
    private Plugins plugins;

    @XmlElementWrapper(name = "optionMappings")
    @XmlElement(name = "mapping")
    private List<Mapping> mappingList;

    @XmlTransient
    private Map<String, String> optionsMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    @XmlTransient
    private Map<String, String> pluginsMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    /**
     * Returns the name of the profile
     *
     * @return the name of the profile
     */
    String getName() {
        return StringUtils.trim(name);
    }

    /**
     * Returns the protocol associated to this profile. (optional)
     *
     * @return (optional) the protocol associated to this profile
     */
    String getProtocol() {
        return protocol;
    }

    /**
     * Returns the name of the class to handle this profile. (optional)
     *
     * @return (optional) the name of the class that handles this profile
     */
    String getHandler() {
        return handler;
    }

    /**
     * Returns the options map for this profile. (optional)
     *
     * @return (optional) the options map for this profile
     */
    Map<String, String> getOptionsMap() {
        return optionsMap;
    }

    /**
     * Returns the map of plugins configured for this profile
     *
     * @return the map of plugins configured for this profile
     */
    Map<String, String> getPluginsMap() {
        return pluginsMap;
    }

    List<Mapping> getMappingList() {
        return mappingList;
    }

    @XmlElement(name = "plugins")
    private void setPlugins(Plugins plugins) {
        this.plugins = plugins;

        if (plugins != null) {
            String fragmenter = plugins.getFragmenter();
            if (StringUtils.isNotBlank(fragmenter)) {
                pluginsMap.put(Profile.Plugins.FRAGMENTER, fragmenter);
            }

            String accessor = plugins.getAccessor();
            if (StringUtils.isNotBlank(accessor)) {
                pluginsMap.put(Profile.Plugins.ACCESSOR, accessor);
            }

            String resolver = plugins.getResolver();
            if (StringUtils.isNotBlank(resolver)) {
                pluginsMap.put(Profile.Plugins.RESOLVER, resolver);
            }

            String metadata = plugins.getMetadata();
            if (StringUtils.isNotBlank(metadata)) {
                pluginsMap.put(Profile.Plugins.METADATA, metadata);
            }

            String outputFormat = plugins.getOutputFormat();
            if (StringUtils.isNotBlank(outputFormat)) {
                pluginsMap.put(Profile.Plugins.OUTPUTFORMAT, outputFormat);
            }
        }
    }

    private Plugins getPlugins() {
        return plugins;
    }

    /**
     * Plugins identify the fully qualified name of Java
     * classes that PXF uses to parse and access the
     * external data.
     */
    @XmlRootElement(name = "plugins")
    @XmlAccessorType(XmlAccessType.FIELD)
    static class Plugins {

        final static String FRAGMENTER = "FRAGMENTER";
        final static String ACCESSOR = "ACCESSOR";
        final static String RESOLVER = "RESOLVER";
        final static String METADATA = "METADATA";
        final static String OUTPUTFORMAT = "OUTPUTFORMAT";

        @XmlElement(name = "fragmenter", required = true)
        private String fragmenter;

        @XmlElement(name = "accessor", required = true)
        private String accessor;

        @XmlElement(name = "resolver", required = true)
        private String resolver;

        @XmlElement(name = "metadata")
        private String metadata;

        @XmlElement(name = "outputFormat")
        private String outputFormat;

        /**
         * Returns the fully qualified class name for the Profile's fragmenter
         *
         * @return the fully qualified class name for the Profile's fragmenter
         */
        String getFragmenter() {
            return fragmenter;
        }

        /**
         * Returns the fully qualified class name for the Profile's accessor
         *
         * @return the fully qualified class name for the Profile's accessor
         */
        String getAccessor() {
            return accessor;
        }

        /**
         * Returns the fully qualified class name for the Profile's resolver
         *
         * @return the fully qualified class name for the Profile's resolver
         */
        String getResolver() {
            return resolver;
        }

        /**
         * Returns the fully qualified class name for the Profile's metadata
         *
         * @return the fully qualified class name for the Profile's metadata
         */
        String getMetadata() {
            return metadata;
        }

        /**
         * Returns the fully qualified class name for the Profile's output format
         *
         * @return the fully qualified class name for the Profile's output format
         */
        String getOutputFormat() {
            return outputFormat;
        }
    }

    /**
     * A mapping defines an option that is allowed
     * for the given profile. The option maps to a property that
     * can be interpreted by the Profile Plugins.
     */
    @XmlRootElement(name = "mapping")
    @XmlAccessorType(XmlAccessType.FIELD)
    static class Mapping {

        @XmlAttribute(name = "option")
        private String option;

        @XmlAttribute(name = "property")
        private String property;

        /**
         * Returns the allowlisted option
         *
         * @return the allowlisted option
         */
        String getOption() {
            return option;
        }

        /**
         * Returns the name of the property for the given option
         *
         * @return the name of the property for the given option
         */
        String getProperty() {
            return property;
        }
    }
}
