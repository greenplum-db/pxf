package org.greenplum.pxf.automation.testplugin;

import java.util.List;

import org.greenplum.pxf.api.Fragment;
import org.greenplum.pxf.api.Fragmenter;
import org.greenplum.pxf.api.utilities.InputData;
import org.greenplum.pxf.api.FilterParser;


/**
 * Test class for regression tests.
 * The only thing this class does is to take received filter string from GPDB (HAS-FILTER & FILTER).
 * And return it in UserData back to gpdb for later validation in Resolver/Accessor
 */
public class FilterVerifyFragmenter extends Fragmenter
{
    private static class TestFilterBuilder implements FilterParser.FilterBuilder {
        public Object build(FilterParser.Operation operation, Object left, Object right) throws Exception {return new Object();};
        public Object build(FilterParser.Operation operation, Object operand) throws Exception {return new Object();};
        public Object build(FilterParser.LogicalOperation operation, Object left, Object right) throws Exception {return new Object();};
        public Object build(FilterParser.LogicalOperation operation, Object filter) throws Exception {return new Object();};
    }

    public FilterVerifyFragmenter(InputData input) {
        super(input);
    }

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
        if (inputData.hasFilter()) {
            filter = inputData.getFilterString();
            FilterParser parser = new FilterParser(new TestFilterBuilder());
            parser.parse(filter.getBytes(FilterParser.DEFAULT_CHARSET));
        }

        String [] hosts =  {"localhost" , "localhost" , "localhost"};

        // Set filter value as returned user data.
        Fragment fragment = new Fragment("dummy_file_path",
                hosts,
                new String().getBytes(),
                filter.getBytes());
        fragments.add(fragment);

        return fragments;
    }
}
