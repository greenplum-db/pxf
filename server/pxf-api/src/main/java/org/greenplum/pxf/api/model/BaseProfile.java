package org.greenplum.pxf.api.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@XmlRootElement(name = "profile")
@XmlAccessorType(XmlAccessType.PROPERTY)
public class BaseProfile implements Profile {

    private String name;
    private Plugins plugins;
    private String protocol;
    private List<Mapping> mappingList;
    private Map<String, String> optionMappingTable = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    @Override
    public String getName() {
        return name;
    }

    @XmlElement(name = "name", required = true)
    private void setName(String name) {
        this.name = name;
    }

    @Override
    public Map<String, String> getPluginTable() {
        return plugins.toMap();
    }

    private Plugins getPlugins() {
        return plugins;
    }

    @XmlElement(name = "plugins")
    private void setPlugins(Plugins plugins) {
        this.plugins = plugins;
    }

    @Override
    public String getProtocol() {
        return protocol;
    }

    @XmlElement(name = "protocol")
    private void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    @Override
    public Map<String, String> getOptionMappingTable() {
        return optionMappingTable;
    }

    public List<Mapping> getOptionMappings() {
        return mappingList;
    }

    @XmlElementWrapper(name = "optionMappings")
    @XmlElement(name = "mapping")
    public void setOptionMappings(List<Mapping> mappingList) {
        this.mappingList = mappingList;

        if (mappingList != null) {
            for (Mapping mapping : mappingList) {
                optionMappingTable.put(mapping.option, mapping.property);
            }
        }
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

    @XmlRootElement(name = "mapping")
    @XmlAccessorType(XmlAccessType.PROPERTY)
    private static class Mapping {

        private String option;
        private String property;

        public String getOption() {
            return option;
        }

        @XmlAttribute(name = "option")
        public void setOption(String option) {
            this.option = option;
        }

        public String getProperty() {
            return property;
        }

        @XmlAttribute(name = "property")
        public void setProperty(String property) {
            this.property = property;
        }
    }

}
