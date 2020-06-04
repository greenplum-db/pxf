package org.greenplum.pxf.automation.testplugin;

import org.greenplum.pxf.api.model.BaseFragmenter;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Test class for regression tests.
 * Stringify column projection information when available as a comma separated list of column names.
 * Return it as UserData
 */
@Component("ColumnProjectionVerifyFragmenter")
@RequestScope
public class ColumnProjectionVerifyFragmenter extends BaseFragmenter {

    /**
     * Returns one fragment with incoming column projection column names as CSV in the user data.
     * If no incoming column projection info available, then return "No Column Projection" as user data.
     *
     * @return one data fragment
     */
    @Override
    public List<Fragment> getFragments() {

        String columnProjection = "No Column Projection";

        if (context.getNumAttrsProjected() > 0) {
            columnProjection = context.getTupleDescription().stream()
                    .filter(ColumnDescriptor::isProjected)
                    .map(ColumnDescriptor::columnName)
                    .collect(Collectors.joining("|"));
        }

        String[] hosts = {"localhost", "localhost", "localhost"};
        // Set filter value as returned user data.
        Fragment fragment = new Fragment("dummy_file_path", hosts,
                new ColumnProjectionVerifyFragmentMetadata(columnProjection));
        fragments.add(fragment);

        return fragments;
    }
}
