package org.greenplum.pxf.automation.testplugin;

import org.greenplum.pxf.api.utilities.InputData;
import org.greenplum.pxf.plugins.hive.HiveDataFragmenter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.Field;

public class HiveDataFragmenterWithFilter extends HiveDataFragmenter {

    private static final Log LOG = LogFactory.getLog(HiveDataFragmenterWithFilter.class);

    public HiveDataFragmenterWithFilter(InputData inputData) {
        super(inputData);
        addFilters();  // Set the test hive filter (overwrite gpdb filter)
    }

    /*
     *  Ignores filter from gpdb, use user defined filter
     *  Set the protected filterString by reflection (only for regression, dont want to modify the original code)
     */
    private void addFilters() {

        String filterStr = inputData.getUserProperty("TEST-HIVE-FILTER");
        LOG.debug("user defined filter: " + filterStr);
        if ((filterStr == null) || filterStr.isEmpty() || "null".equals(filterStr))
            return;

        try {
            Field protectedField = InputData.class.getDeclaredField("filterString");
            protectedField.setAccessible(true);
            protectedField.set(inputData, filterStr);
            LOG.debug("User defined filter: " + inputData.getFilterString());

            Field protectedField1 = InputData.class.getDeclaredField("filterStringValid");
            protectedField1.setAccessible(true);
            protectedField1.setBoolean(inputData,true);
            LOG.debug("User defined filter: " + inputData.hasFilter());

        } catch (Exception e) {
            LOG.debug(e);
        }
    }
}
