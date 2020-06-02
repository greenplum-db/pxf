package org.greenplum.pxf.service;

import org.springframework.core.task.TaskRejectedException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public class PxfThreadPoolTaskExecutor extends ThreadPoolTaskExecutor {

    @Override
    public Future<?> submit(Runnable task) {
        try {
            return super.submit(task);
        } catch (TaskRejectedException ex) {
            throw new TaskRejectedException("Server processing capacity exceeded. Consider increasing the values of PXF_TASK_POOL_MAX_SIZE and/or PXF_TASK_POOL_QUEUE_CAPACITY in $PXF_CONF/conf/pxf-env.sh",
                    ex.getCause());
        }
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        try {
            return super.submit(task);
        } catch (TaskRejectedException ex) {
            throw new TaskRejectedException("Server processing capacity exceeded. Consider increasing the values of PXF_TASK_POOL_MAX_SIZE and/or PXF_TASK_POOL_QUEUE_CAPACITY in $PXF_CONF/conf/pxf-env.sh",
                    ex.getCause());
        }
    }
}
