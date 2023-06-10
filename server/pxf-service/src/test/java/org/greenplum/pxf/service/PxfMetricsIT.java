package org.greenplum.pxf.service;

import com.google.common.base.Charsets;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.service.controller.ReadService;
import org.greenplum.pxf.service.controller.WriteService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;
import org.springframework.boot.test.autoconfigure.actuate.metrics.AutoConfigureMetrics;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.util.MultiValueMap;

import java.io.OutputStream;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.condition.OS.MAC;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = PxfServiceApplication.class)
@AutoConfigureMetrics
// on MacOS with Intel chip (and JDK 8) this test crashes the JVM. Commenting out for now to not run on Mac.
@DisabledOnOs(MAC)
public class PxfMetricsIT {

    @LocalServerPort
    private int port;

    @MockBean
    private RequestParser<MultiValueMap<String, String>> mockParser;

    @MockBean
    private ReadService readService;

    @MockBean
    private WriteService mockWriteService;

    @Mock
    private RequestContext mockContext;

    private WebTestClient client;

    @BeforeEach
    public void setUp() {
        client = WebTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
    }

    @Test
    public void test_HttpServerRequests_Metric() throws Exception {
        mockServices();
        // call PXF read API
        client.get().uri("/pxf/read")
                .header("X-GP-ENCODED-HEADER-VALUES", "true")
                .header("X-GP-USER", "reader")
                .header("X-GP-SEGMENT-ID", "77")
                .header("X-GP-OPTIONS-PROFILE", "profile%3Atest")
                .header("X-GP-OPTIONS-SERVER", "speedy")
                .exchange().expectStatus().isOk()
                .expectBody(String.class).isEqualTo("Hello from read!");

        // assert metric got reported with proper tags
        client.get().uri("/actuator/metrics/http.server.requests?tag=uri:/pxf/read")
                .exchange().expectStatus().isOk().expectBody()
                .jsonPath("$.measurements[?(@.statistic == 'COUNT')].value").isEqualTo(1.0)
                .jsonPath("$.measurements[?(@.statistic == 'TOTAL_TIME' && @.value < 0.5)]").doesNotHaveJsonPath()
                .jsonPath("$.availableTags[?(@.tag == 'application')].values[0]").isEqualTo("pxf-service")
                .jsonPath("$.availableTags[?(@.tag == 'user')].values[0]").isEqualTo("reader")
                .jsonPath("$.availableTags[?(@.tag == 'segment')].values[0]").isEqualTo("77")
                .jsonPath("$.availableTags[?(@.tag == 'profile')].values[0]").isEqualTo("profile:test")
                .jsonPath("$.availableTags[?(@.tag == 'server')].values[0]").isEqualTo("speedy");

        // call PXF write API
        client.post().uri("/pxf/write")
                .header("X-GP-USER", "writer")
                .header("X-GP-SEGMENT-ID", "77")
                .header("X-GP-OPTIONS-PROFILE", "profile:test")
                .header("X-GP-OPTIONS-SERVER", "speedy")
                .exchange().expectStatus().isOk()
                .expectBody(String.class).isEqualTo("Hello from write!");

        // assert metric got reported with proper tags
        client.get().uri("/actuator/metrics/http.server.requests?tag=uri:/pxf/write")
                .exchange().expectStatus().isOk().expectBody()
                .jsonPath("$.measurements[?(@.statistic == 'COUNT')].value").isEqualTo(1.0)
                .jsonPath("$.availableTags[?(@.tag == 'application')].values[0]").isEqualTo("pxf-service")
                .jsonPath("$.availableTags[?(@.tag == 'user')].values[0]").isEqualTo("writer")
                .jsonPath("$.availableTags[?(@.tag == 'segment')].values[0]").isEqualTo("77")
                .jsonPath("$.availableTags[?(@.tag == 'profile')].values[0]").isEqualTo("profile:test")
                .jsonPath("$.availableTags[?(@.tag == 'server')].values[0]").isEqualTo("speedy");

        // assert metric for segment access is aggregate
        client.get().uri("/actuator/metrics/http.server.requests?tag=segment:77")
                .exchange().expectStatus().isOk().expectBody()
                .jsonPath("$.measurements[?(@.statistic == 'COUNT')].value").isEqualTo(2.0)
                .jsonPath("$.availableTags[?(@.tag == 'application')].values[0]").isEqualTo("pxf-service")
                .jsonPath("$.availableTags[?(@.tag == 'user')].values[0]").isEqualTo("reader")
                .jsonPath("$.availableTags[?(@.tag == 'user')].values[1]").isEqualTo("writer")
                .jsonPath("$.availableTags[?(@.tag == 'profile')].values[0]").isEqualTo("profile:test")
                .jsonPath("$.availableTags[?(@.tag == 'server')].values[0]").isEqualTo("speedy");

        // hit the actuator health endpoint
        client.get().uri("/actuator/health")
                .exchange().expectStatus().isOk().expectBody()
                .json("{\"status\":\"UP\",\"groups\":[\"liveness\",\"readiness\"]}");

        // assert prometheus endpoint reflects the metric as well
        String prometheusResponse = client.get().uri("/actuator/prometheus")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class).returnResult().getResponseBody();
        assertNotNull(prometheusResponse);
        assertTrue(prometheusResponse.contains("http_server_requests_seconds_count{application=\"pxf-service\",exception=\"None\",method=\"GET\",outcome=\"SUCCESS\",profile=\"profile:test\",segment=\"77\",server=\"speedy\",status=\"200\",uri=\"/pxf/read\",user=\"reader\",} 1.0\n"));
        assertTrue(prometheusResponse.contains("http_server_requests_seconds_count{application=\"pxf-service\",exception=\"None\",method=\"POST\",outcome=\"SUCCESS\",profile=\"profile:test\",segment=\"77\",server=\"speedy\",status=\"200\",uri=\"/pxf/write\",user=\"writer\",} 1.0\n"));
        assertTrue(prometheusResponse.contains("http_server_requests_seconds_count{application=\"pxf-service\",exception=\"None\",method=\"GET\",outcome=\"SUCCESS\",profile=\"unknown\",segment=\"unknown\",server=\"unknown\",status=\"200\",uri=\"/actuator/health\",user=\"unknown\",} 1.0\n"));
    }

    private void mockServices() throws Exception {
        // mock ReadService
        when(mockParser.parseRequest(any(), eq(RequestContext.RequestType.READ_BRIDGE))).thenReturn(mockContext);
        Answer<Void> readAnswer = invocation -> {
            // sleep to simulate time it takes to execute, check that reported metric takes into account async time
            Thread.sleep(500);
            invocation.getArgument(1, OutputStream.class).write("Hello from read!".getBytes(Charsets.UTF_8));
            return null;
        };
        doAnswer(readAnswer).when(readService).readData(any(), any());

        // mock WriteService
        when(mockParser.parseRequest(any(), eq(RequestContext.RequestType.WRITE_BRIDGE))).thenReturn(mockContext);
        when(mockWriteService.writeData(same(mockContext), any())).thenReturn("Hello from write!");
    }

}
