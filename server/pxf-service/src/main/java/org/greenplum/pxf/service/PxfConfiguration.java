package org.greenplum.pxf.service;

import org.greenplum.pxf.api.configuration.PxfServerProperties;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.task.TaskExecutionProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.task.TaskExecutorBuilder;
import org.springframework.boot.task.TaskExecutorCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.TaskDecorator;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.AsyncAnnotationBeanPostProcessor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Declares the registerSecurityServletFilter bean method to be processed by
 * the Spring container
 */
@Configuration
@EnableConfigurationProperties(PxfServerProperties.class)
public class PxfConfiguration implements WebMvcConfigurer {

    /**
     * Bean name of PXF's {@link TaskExecutor}.
     */
    public static final String PXF_APPLICATION_TASK_EXECUTOR_BEAN_NAME = "pxfApplicationTaskExecutor";

    private final ListableBeanFactory beanFactory;

    /**
     * Constructs a PXF Configuration object with the provided
     * {@link ListableBeanFactory}
     *
     * @param beanFactory the beanFactory
     */
    public PxfConfiguration(ListableBeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }

    /**
     * Configures the TaskExecutor to be used for async requests (i.e. Bridge
     * Read).
     */
    @Override
    public void configureAsyncSupport(AsyncSupportConfigurer configurer) {
        Object taskExecutor = this.beanFactory
                .getBean(PXF_APPLICATION_TASK_EXECUTOR_BEAN_NAME);
        if (taskExecutor instanceof AsyncTaskExecutor) {
            configurer.setTaskExecutor(((AsyncTaskExecutor) taskExecutor));
        }
    }

    /**
     * Configures and builds the {@link ThreadPoolTaskExecutor}
     *
     * @return the {@link ThreadPoolTaskExecutor}
     */
    @Lazy
    @Bean(name = {PXF_APPLICATION_TASK_EXECUTOR_BEAN_NAME,
            AsyncAnnotationBeanPostProcessor.DEFAULT_TASK_EXECUTOR_BEAN_NAME})
    public ThreadPoolTaskExecutor pxfApplicationTaskExecutor(PxfServerProperties pxfServerProperties,
                                                             ObjectProvider<TaskExecutorCustomizer> taskExecutorCustomizers,
                                                             ObjectProvider<TaskDecorator> taskDecorator) {

        TaskExecutionProperties properties = pxfServerProperties.getTask();
        TaskExecutionProperties.Pool pool = properties.getPool();
        TaskExecutorBuilder builder = new TaskExecutorBuilder();
        builder = builder.queueCapacity(pool.getQueueCapacity());
        builder = builder.corePoolSize(pool.getCoreSize());
        builder = builder.maxPoolSize(pool.getMaxSize());
        builder = builder.allowCoreThreadTimeOut(pool.isAllowCoreThreadTimeout());
        builder = builder.keepAlive(pool.getKeepAlive());
        TaskExecutionProperties.Shutdown shutdown = properties.getShutdown();
        builder = builder.awaitTermination(shutdown.isAwaitTermination());
        builder = builder.awaitTerminationPeriod(shutdown.getAwaitTerminationPeriod());
        builder = builder.threadNamePrefix(properties.getThreadNamePrefix());
        builder = builder.customizers(taskExecutorCustomizers.orderedStream()::iterator);
        builder = builder.taskDecorator(taskDecorator.getIfUnique());
        return builder.build(PxfThreadPoolTaskExecutor.class);
    }
}
