package org.greenplum.pxf.api.model;

import org.apache.hadoop.conf.Configuration;

public class HadoopConfigurationFactory implements ConfigurationFactory<Configuration> {

    @Override
    public Configuration getConfiguration(InputData inputData) {
        return null;
    }
}
