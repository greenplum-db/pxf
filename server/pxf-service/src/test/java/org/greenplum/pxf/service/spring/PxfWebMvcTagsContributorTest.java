package org.greenplum.pxf.service.spring;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.apache.commons.collections.CollectionUtils;
import org.greenplum.pxf.service.HttpHeaderDecoder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PxfWebMvcTagsContributorTest {

    private PxfWebMvcTagsContributor contributor;
    private MockHttpServletRequest mockRequest;

    @BeforeEach
    public void setup() {
        contributor = new PxfWebMvcTagsContributor(new HttpHeaderDecoder());
        mockRequest = new MockHttpServletRequest();
    }

    @Test
    public void testPxfWebMvcTagsContributor_pxfEndpoint_namedServer() {
        mockRequest.addHeader("X-GP-USER", "Alex");
        mockRequest.addHeader("X-GP-SEGMENT-ID", "5");
        mockRequest.addHeader("X-GP-OPTIONS-PROFILE", "test:text");
        mockRequest.addHeader("X-GP-OPTIONS-SERVER", "test_server");

        List<Tag> expectedTags = Tags.of("user", "Alex")
                .and("segment", "5")
                .and("profile", "test:text")
                .and("server", "test_server")
                .stream().collect(Collectors.toList());

        Iterable<Tag> tagsIterable = contributor.getTags(mockRequest, null, null, null);
        List<Tag> tags = StreamSupport.stream(tagsIterable.spliterator(), false).collect(Collectors.toList());

        assertTrue(CollectionUtils.isEqualCollection(expectedTags, tags));
        assertFalse(contributor.getLongRequestTags(mockRequest, null).iterator().hasNext());
    }

    @Test
    public void testPxfWebMvcTagsContributor_pxfEndpoint_defaultServer() {
        mockRequest.addHeader("X-GP-USER", "Alex");
        mockRequest.addHeader("X-GP-SEGMENT-ID", "5");
        mockRequest.addHeader("X-GP-OPTIONS-PROFILE", "test:text");

        List<Tag> expectedTags = Tags.of("user", "Alex")
                .and("segment", "5")
                .and("profile", "test:text")
                .and("server", "default")
                .stream().collect(Collectors.toList());

        Iterable<Tag> tagsIterable = contributor.getTags(mockRequest, null, null, null);
        List<Tag> tags = StreamSupport.stream(tagsIterable.spliterator(), false).collect(Collectors.toList());

        assertTrue(CollectionUtils.isEqualCollection(expectedTags, tags));
        assertFalse(contributor.getLongRequestTags(mockRequest, null).iterator().hasNext());
    }

    @Test
    public void testPxfWebMvcTagsContributor_pxfEndpoint_encoded() {
        mockRequest.addHeader("X-GP-ENCODED-HEADER-VALUES", "true");
        mockRequest.addHeader("X-GP-USER", "Alex");
        mockRequest.addHeader("X-GP-SEGMENT-ID", "5");
        mockRequest.addHeader("X-GP-OPTIONS-PROFILE", "test%3Atext");
        mockRequest.addHeader("X-GP-OPTIONS-SERVER", "test_server");

        List<Tag> expectedTags = Tags.of("user", "Alex")
                .and("segment", "5")
                .and("profile", "test:text")
                .and("server", "test_server")
                .stream().collect(Collectors.toList());

        Iterable<Tag> tagsIterable = contributor.getTags(mockRequest, null, null, null);
        List<Tag> tags = StreamSupport.stream(tagsIterable.spliterator(), false).collect(Collectors.toList());

        assertTrue(CollectionUtils.isEqualCollection(expectedTags, tags));
        assertFalse(contributor.getLongRequestTags(mockRequest, null).iterator().hasNext());
    }

    @Test
    public void testPxfWebMvcTagsContributor_nonPxfEndpoint() {
        List<Tag> expectedTags = Tags.of("user", "unknown")
                .and("segment", "unknown")
                .and("profile", "unknown")
                .and("server", "unknown")
                .stream().collect(Collectors.toList());

        Iterable<Tag> tagsIterable = contributor.getTags(mockRequest, null, null, null);
        List<Tag> tags = StreamSupport.stream(tagsIterable.spliterator(), false).collect(Collectors.toList());

        assertTrue(CollectionUtils.isEqualCollection(expectedTags, tags));
        assertFalse(contributor.getLongRequestTags(mockRequest, null).iterator().hasNext());
    }

}
