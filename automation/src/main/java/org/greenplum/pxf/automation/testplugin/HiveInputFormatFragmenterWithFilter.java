package org.greenplum.pxf.automation.testplugin;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.greenplum.pxf.plugins.hive.HiveInputFormatFragmenter;

public class HiveInputFormatFragmenterWithFilter extends HiveInputFormatFragmenter {

    private static final Log LOG = LogFactory.getLog(HiveInputFormatFragmenterWithFilter.class);

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
        addFilters();  // Set the test hive filter (overwrite gpdb filter)
    }

    /*
     *  Ignores filter from gpdb, use user defined filter
     *  Set the protected filterString by reflection (only for regression, dont want to modify the original code)
     */
    private void addFilters() {

        // TODO: allowlist the option
        String filterStr = context.getOption("TEST-HIVE-FILTER");
        LOG.debug("user defined filter: " + filterStr);
        if ((filterStr == null) || filterStr.isEmpty() || "null".equals(filterStr))
            return;

        context.setFilterString(filterStr);
        LOG.debug("User defined filter: " + context.getFilterString());

        LOG.debug("User defined filter: " + context.hasFilter());
    }
}
