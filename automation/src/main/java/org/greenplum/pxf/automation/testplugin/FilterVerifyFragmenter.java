package org.greenplum.pxf.automation.testplugin;

import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.model.BaseFragmenter;
import org.greenplum.pxf.api.model.Fragment;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import java.util.List;

/**
 * Test class for regression tests.
 * The only thing this class does is to take received filter string from GPDB (FILTER).
 * And return it in UserData back to gpdb for later validation in Resolver/Accessor
 */
@Component("FilterVerifyFragmenter")
@RequestScope
public class FilterVerifyFragmenter extends BaseFragmenter {

    /**
     * Returns one fragment with incoming filter string value as the user data.
     * If no incoming filter, then return "No Filter" as user data.
     *
     * @return one data fragment
     * @throws Exception
     */
    @Override
    public List<Fragment> getFragments() throws Exception {

        String filter = "No filter";

        // Validate the filterstring by parsing using a dummy filterBuilder
        if (context.hasFilter()) {
            filter = context.getFilterString();
            new FilterParser().parse(filter);
        }

        String[] hosts = {"localhost", "localhost", "localhost"};

        // Set filter value as returned user data.
        Fragment fragment = new Fragment("dummy_file_path", hosts,
                new FilterVerifyFragmentMetadata(filter));
        fragments.add(fragment);

        return fragments;
    }
}
