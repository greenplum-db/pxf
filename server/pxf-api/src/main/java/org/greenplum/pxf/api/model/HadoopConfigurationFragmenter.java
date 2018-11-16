package org.greenplum.pxf.api.model;

import org.apache.hadoop.conf.Configuration;

// TODO move to HDFS package, override methods
public class HadoopConfigurationFragmenter extends BaseFragmenter {

    protected Configuration configuration;

    private HadoopConfigurationHelper configHelper = new HadoopConfigurationHelper();

    @Override
    public void initialize(InputData inputData) {
        super.initialize(inputData);

        // fetch configuration based on provided input data
        configuration = configHelper.getConfiguration(inputData);
    }


    // ------------- PACKAGE LEVEL FOR TESTING -------------------

    void setConfigHelper(HadoopConfigurationHelper configHelper) {
        this.configHelper = configHelper;
    }

}
