package org.greenplum.pxf.automation.smoke;

import org.greenplum.pxf.automation.BaseFunctionality;

/** Smoke Tests Base class */
public abstract class BaseSmoke extends BaseFunctionality {
    // Method to be overridden by all extending classes
    protected abstract void prepareData() throws Exception;

    protected abstract void createTables() throws Exception;

    protected abstract void queryResults() throws Exception;

    /**
     * Logic order of a smoke test run
     *
     * @throws Exception
     */
    public void runTest() throws Exception {
        prepareData();
        createTables();
        queryResults();
    }
}