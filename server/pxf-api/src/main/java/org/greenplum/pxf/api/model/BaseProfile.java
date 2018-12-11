package org.greenplum.pxf.api.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@XmlRootElement(name = "profile")
@XmlAccessorType(XmlAccessType.FIELD)
public class BaseProfile implements Profile {

    @XmlElement(name = "name", required = true)
    private String name;

    @XmlElement(name = "protocol")
    private String protocol;

    @XmlElement(name = "plugins")
    private Plugins plugins;

    @XmlElement(name = "optionMappings")
    private List<Mapping> mappings;

    private Map<String, String> optionMappings = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public Map<String, String> getPlugins() {
        return plugins.toMap();
    }

    @Override
    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    @Override
    public Map<String, String> getOptionMappings() {
        // TODO: proper caching / synchronization
        Map<String, String> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (mappings != null) {
            for (Mapping mapping : mappings) {
                result.put(mapping.option, mapping.property);
            }
        }
        return result;
    }

    @XmlRootElement(name = "plugins")
    @XmlAccessorType(XmlAccessType.PROPERTY)
    private static class Plugins {

        private String fragmenter;
        private String accessor;
        private String resolver;
        private String metadata;
        private String outputFormat;
        private Map<String, String> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        public String getFragmenter() {
            return fragmenter;
        }

        @XmlElement(name = "fragmenter", required = true)
        public void setFragmenter(String fragmenter) {
            this.fragmenter = fragmenter;
            map.put("fragmenter", fragmenter);
        }

        public String getAccessor() {
            return accessor;
        }

        @XmlElement(name = "accessor", required = true)
        public void setAccessor(String accessor) {
            this.accessor = accessor;
            map.put("accessor", accessor);
        }

        public String getResolver() {
            return resolver;
        }

        @XmlElement(name = "resolver", required = true)
        public void setResolver(String resolver) {
            this.resolver = resolver;
            map.put("resolver", resolver);
        }

        public String getMetadata() {
            return metadata;
        }

        @XmlElement(name = "metadata")
        public void setMetadata(String metadata) {
            this.metadata = metadata;
            map.put("metadata", metadata);
        }

        public String getOutputFormat() {
            return outputFormat;
        }

        @XmlElement(name = "outputFormat")
        public void setOutputFormat(String outputFormat) {
            this.outputFormat = outputFormat;
            map.put("outputFormat", outputFormat);
        }

        public Map<String, String> toMap() {
            return map;
        }
    }


    @XmlAccessorType(XmlAccessType.FIELD)
    private static class Mapping {
        @XmlAttribute(name = "option")
        String option;

        @XmlAttribute(name = "property")
        String property;

        public Mapping() {
            // for debugging
            int b = 1+3;
            int a =b;
        }
    }

}
