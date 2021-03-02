package org.greenplum.pxf.service.rest;

import com.google.common.base.Charsets;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.service.HttpHeaderDecoder;
import org.greenplum.pxf.service.RequestParser;
import org.greenplum.pxf.service.controller.ReadService;
import org.greenplum.pxf.service.controller.WriteService;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest({PxfResource.class, PxfLegacyResource.class})
public class PxfResourceIT {

    @Autowired
    private MockMvc mvc;

    @MockBean
    private RequestParser<MultiValueMap<String, String>> mockParser;

    @MockBean
    private WriteService mockWriteService;

    @MockBean
    private HttpHeaderDecoder mockHttpHeaderDecoder;

    @Mock
    private RequestContext mockContext;

    @Test
    public void testReadEndpoint() throws Exception {
        when(mockParser.parseRequest(any(), eq(RequestContext.RequestType.READ_BRIDGE))).thenReturn(mockContext);

        ResultActions result = mvc.perform(get("/pxf/read")).andExpect(status().isOk());
        Thread.sleep(200);
        result.andExpect(content().string("Hello from read!"));
    }

    @Test
    public void testWriteEndpoint() throws Exception {
        when(mockParser.parseRequest(any(), eq(RequestContext.RequestType.WRITE_BRIDGE))).thenReturn(mockContext);
        when(mockWriteService.writeData(same(mockContext), any())).thenReturn("Hello from write!");

        mvc.perform(post("/pxf/write"))
                .andExpect(status().isOk())
                .andExpect(content().string("Hello from write!"));
    }

    @Test
    public void testLegacyFragmenterEndpoint() throws Exception {
        ResultActions result = mvc.perform(
                get("/pxf/v15/Fragmenter/getFragments").contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isInternalServerError());
        Thread.sleep(200);
        result.andExpect(r -> assertTrue(r.getResolvedException() instanceof PxfRuntimeException))
                .andExpect(r -> assertEquals("getFragments API (v15) is no longer supported by the server, upgrade PXF extension (run 'pxf [cluster] register' and then 'ALTER EXTENSION pxf UPDATE')",
                        r.getResolvedException().getMessage()));
        result.andExpect(content().string("upgrade PXF extension (run 'pxf [cluster] register' and then 'ALTER EXTENSION pxf UPDATE')"));
    }

    @Test
    public void testLegacyBridgeEndpoint() throws Exception {
        //TODO: legacy endpoint should throw 500 exception with a hint, validate error message
        when(mockParser.parseRequest(any(), eq(RequestContext.RequestType.READ_BRIDGE))).thenReturn(mockContext);

        ResultActions result = mvc.perform(get("/pxf/v15/Bridge")).andExpect(status().isOk());
        Thread.sleep(200);
        result.andExpect(content().string("Hello from read!"));
    }

    @Test
    public void testLegacyWritableEndpoint() throws Exception {
        //TODO: legacy endpoint should throw 500 exception with a hint, validate error message
        when(mockParser.parseRequest(any(), eq(RequestContext.RequestType.WRITE_BRIDGE))).thenReturn(mockContext);
        when(mockWriteService.writeData(same(mockContext), any())).thenReturn("Hello from write!");

        mvc.perform(post("/pxf/v15/Writable/stream"))
                .andExpect(status().isOk())
                .andExpect(content().string("Hello from write!"));
    }

    @TestConfiguration
    static class PxfResourceTestConfiguration {
        @Bean
        ReadService createReadService() {
            return (ctx, out) -> out.write("Hello from read!".getBytes(Charsets.UTF_8));
        }

        @ControllerAdvice
        public class ExceptionController {

            @ExceptionHandler(PxfRuntimeException.class)
            @ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR)
            @ResponseBody
            public ResponseEntity<String> handleException(PxfRuntimeException ex) {
                // print hint into the body so we can assert this in the tests
                return new ResponseEntity<>(ex.getHint(), HttpStatus.INTERNAL_SERVER_ERROR);
            }
        }
    }
}
