package listeners;

import annotations.WorksWithFDW;
import org.greenplum.pxf.automation.utils.system.FDWUtils;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestResult;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

/**
 * Method invocation listener that skips tests that are not annotated as working with FDW when ran in FDW context.
 */
public class FDWSkipTestAnalyzer implements IInvokedMethodListener {

    @Override
    public void beforeInvocation(IInvokedMethod invokedMethod, ITestResult result) {
        Method method = result.getMethod().getConstructorOrMethod().getMethod();
        if (method == null) {
            return;
        }
        // check only @Test annotated method, not @Before.. and @After.. ones
        if (FDWUtils.useFDW && method.isAnnotationPresent(Test.class) && !method.isAnnotationPresent(WorksWithFDW.class)) {
            throw new SkipException("The test is not supported in FDW mode");
        }
    }

    @Override
    public void afterInvocation(IInvokedMethod iInvokedMethod, ITestResult iTestResult) {
    }
}