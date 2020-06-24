package org.greenplum.pxf.service;

import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static org.greenplum.pxf.api.configuration.PxfServerProperties.PXF_CONF_PROPERTY;

/**
 * A {@link ThreadPoolTaskExecutor} that enhances error reporting when a
 * {@link TaskRejectedException} occurs. The error messages provide hints on
 * how to overcome {@link TaskRejectedException} errors, by suggesting tuning
 * parameters for PXF.
 */
public class PxfThreadPoolTaskExecutor extends ThreadPoolTaskExecutor {

    private static final String PXF_SERVER_PROCESSING_CAPACITY_EXCEEDED_MESSAGE = "PXF Server processing capacity exceeded.";
    private static final String PXF_SERVER_PROCESSING_CAPACITY_EXCEEDED_HINT = "Consider increasing the values of PXF_TASK_POOL_MAX_SIZE and/or PXF_TASK_POOL_QUEUE_CAPACITY in '%s/conf/pxf-env.sh'";

    /**
     * Submits a {@link Runnable} to the executor. Handles
     * {@link TaskRejectedException} errors by enhancing error reporting.
     *
     * @param task the {@code Runnable} to execute (never {@code null})
     * @return a Future representing pending completion of the task
     * @throws TaskRejectedException if the given task was not accepted
     */
    @Override
    public Future<?> submit(Runnable task) {
        try {
            return super.submit(task);
        } catch (TaskRejectedException ex) {
            PxfRuntimeException exception = new PxfRuntimeException(
                    PXF_SERVER_PROCESSING_CAPACITY_EXCEEDED_MESSAGE,
                    String.format(PXF_SERVER_PROCESSING_CAPACITY_EXCEEDED_HINT, System.getProperty(PXF_CONF_PROPERTY)),
                    ex.getCause());
            throw new TaskRejectedException(ex.getMessage(), exception);
        }
    }

    /**
     * Submits a {@link Runnable} to the executor. Handles
     * {@link TaskRejectedException} errors by enhancing error reporting.
     *
     * @param task the {@code Callable} to execute (never {@code null})
     * @return a Future representing pending completion of the task
     * @throws TaskRejectedException if the given task was not accepted
     */
    @Override
    public <T> Future<T> submit(Callable<T> task) {
        try {
            return super.submit(task);
        } catch (TaskRejectedException ex) {
            PxfRuntimeException exception = new PxfRuntimeException(
                    PXF_SERVER_PROCESSING_CAPACITY_EXCEEDED_MESSAGE,
                    String.format(PXF_SERVER_PROCESSING_CAPACITY_EXCEEDED_HINT, System.getProperty(PXF_CONF_PROPERTY)),
                    ex.getCause());
            throw new TaskRejectedException(ex.getMessage(), exception);
        }
    }
}
