package org.greenplum.pxf.service.profile;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

/**
 * Represents a PXF {@link Profile}. A profile is a way
 * to simplify access to external data sources. The
 * {@link Profile} consists of {@link Plugins} a protocol
 * and a list of option mappings
 */
@XmlRootElement(name = "profile")
@XmlAccessorType(XmlAccessType.FIELD)
public class Profile {

    @XmlElement(name = "name", required = true)
    private String name;

    @XmlElement(name = "plugins")
    private Plugins plugins;

    @XmlElement(name = "protocol")
    private String protocol;

    @XmlElementWrapper(name = "optionMappings")
    @XmlElement(name = "mapping")
    private List<Mapping> mappingList;

    /**
     * Returns the name of the profile
     *
     * @return the name of the profile
     */
    String getName() {
        return name;
    }

    /**
     * Returns the Plugins configured for this profile
     *
     * @return the Plugins configured for this profile
     */
    Plugins getPlugins() {
        return plugins;
    }

    /**
     * Returns the protocol associated to this profile. (optional)
     *
     * @return (optional) the protocol associated to this profile
     */
    public String getProtocol() {
        return protocol;
    }

    /**
     * Returns a list of whitelisted option mappings
     *
     * @return a list of whitelisted option mappings
     */
    List<Mapping> getOptionMappings() {
        return mappingList;
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
     * A mapping defines a whitelisted option that is allowed
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
         * Returns the whitelisted option
         *
         * @return the whitelisted option
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
