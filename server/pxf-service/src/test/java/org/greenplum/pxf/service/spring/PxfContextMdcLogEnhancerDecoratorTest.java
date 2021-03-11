package org.greenplum.pxf.service.spring;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.spi.MDCAdapter;
import org.springframework.core.task.TaskDecorator;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PxfContextMdcLogEnhancerDecoratorTest {

    TaskDecorator decorator;

    @Mock
    MDCAdapter mdcMock;

    @BeforeEach
    void setup() {
        decorator = new PxfContextMdcLogEnhancerDecorator();
    }

    @Test
    void testDecorateWithoutContext() throws InterruptedException {

        Map<String, String> contextMap = new HashMap<>();
        when(mdcMock.getCopyOfContextMap()).thenReturn(contextMap);

        Thread thread = new Thread(() -> System.out.println("Run"));
        decorator.decorate(thread).run();
        thread.join();
        verify(mdcMock).getCopyOfContextMap();
        verify(mdcMock).setContextMap(contextMap);
        verify(mdcMock).clear();
        verifyNoMoreInteractions(mdcMock);
    }

    @Test
    void testDecorateWithContext() throws InterruptedException {

        Map<String, String> contextMap = new HashMap<>();
        contextMap.put("foo", "bar");
        when(mdcMock.getCopyOfContextMap()).thenReturn(contextMap);

        Thread thread = new Thread(() -> System.out.println("Run"));
        decorator.decorate(thread).run();
        thread.join();
        verify(mdcMock).getCopyOfContextMap();
        verify(mdcMock).setContextMap(contextMap);
        verify(mdcMock).clear();
        verifyNoMoreInteractions(mdcMock);
    }
}