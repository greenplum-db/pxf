package org.greenplum.pxf.service;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.env.Environment;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class MetricsReporterTest {

    private MeterRegistry registry;
    private MetricsReporter reporter;

    @Mock
    private RequestContext mockContext;
    @Mock
    private Environment mockEnvironment;

    @BeforeEach
    public void setup() {
        // it is not possible to mock the registry as Timer.Builder calls a package-level function that cannot be mocked.
        // so using an actual registry for verification
        registry = new SimpleMeterRegistry();
        reporter = new MetricsReporter(registry, mockEnvironment);
    }

    @Test
    public void testFragmentsSentMetricDisabled() {
        disableFragmentMetrics();

        reporter.reportTimer(MetricsReporter.PxfMetric.FRAGMENTS_SENT, Duration.ofMillis(100), mockContext);
        assertTrue(registry.getMeters().isEmpty());
    }

    @Test
    public void testFragmentsSentMetricEnabled() {
        enableFragmentMetrics();
        when(mockContext.getUser()).thenReturn("Alex");
        when(mockContext.getSegmentId()).thenReturn(5);
        when(mockContext.getProfile()).thenReturn("test:text");
        when(mockContext.getServerName()).thenReturn("test_server");
        Tags expectedTags = Tags.of("user", "Alex").and("segment", "5").and("profile", "test:text").and("server", "test_server");

        reporter.reportTimer(MetricsReporter.PxfMetric.FRAGMENTS_SENT, Duration.ofMillis(100), mockContext);
        Timer timer = registry.get("fragments.sent").tags(expectedTags).timer();
        assertNotNull(timer);
        assertEquals(1, timer.count());
        assertEquals(100, timer.totalTime(TimeUnit.MILLISECONDS));

        reporter.reportTimer(MetricsReporter.PxfMetric.FRAGMENTS_SENT, Duration.ofMillis(51), mockContext);
        timer = registry.get("fragments.sent").tags(expectedTags).timer();
        assertNotNull(timer);
        assertEquals(2, timer.count());
        assertEquals(151, timer.totalTime(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testFragmentsSentMetricEnabledWithOutcome() {
        enableFragmentMetrics();
        when(mockContext.getUser()).thenReturn("Alex");
        when(mockContext.getSegmentId()).thenReturn(5);
        when(mockContext.getProfile()).thenReturn("test:text");
        when(mockContext.getServerName()).thenReturn("test_server");
        Tags expectedTags = Tags.of("user", "Alex").and("segment", "5").and("profile", "test:text")
                .and("server", "test_server").and("outcome", "success");

        // success outcome
        reporter.reportTimer(MetricsReporter.PxfMetric.FRAGMENTS_SENT, Duration.ofMillis(100), mockContext, true);
        Timer timer = registry.get("fragments.sent").tags(expectedTags).timer();
        assertNotNull(timer);
        assertEquals(1, timer.count());
        assertEquals(100, timer.totalTime(TimeUnit.MILLISECONDS));

        // error outcome
        expectedTags = Tags.of("user", "Alex").and("segment", "5").and("profile", "test:text")
                .and("server", "test_server").and("outcome", "error");
        reporter.reportTimer(MetricsReporter.PxfMetric.FRAGMENTS_SENT, Duration.ofMillis(51), mockContext, false);
        timer = registry.get("fragments.sent").tags(expectedTags).timer();
        assertNotNull(timer);
        assertEquals(1, timer.count());
        assertEquals(51, timer.totalTime(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testFragmentsSentMetricEnabledDefaultTagValues() {
        enableFragmentMetrics();
        when(mockContext.getSegmentId()).thenReturn(5);
        Tags expectedTags = Tags.of("user", "unknown").and("segment", "5").and("profile", "unknown").and("server", "default");

        reporter.reportTimer(MetricsReporter.PxfMetric.FRAGMENTS_SENT, Duration.ofMillis(100), mockContext);
        Timer timer = registry.get("fragments.sent").tags(expectedTags).timer();
        assertNotNull(timer);
        assertEquals(1, timer.count());
        assertEquals(100, timer.totalTime(TimeUnit.MILLISECONDS));
    }

    private void enableFragmentMetrics() {
        when(mockEnvironment.getProperty("pxf.metrics.fragments.enabled", Boolean.class, Boolean.FALSE)).thenReturn(true);
    }

    private void disableFragmentMetrics() {
        when(mockEnvironment.getProperty("pxf.metrics.fragments.enabled", Boolean.class, Boolean.FALSE)).thenReturn(false);
    }

}
