
package org.zezutom.processors.nifi.example;

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PostHTTPWithJsonBodyTest {

    private TestRunner testRunner;

    private final Map<String, String> responseAttributes = new HashMap<>();
    {
        responseAttributes.put(HTTPClient.ATT_RES_BODY, "{\"message\": \"ok\"}");
        responseAttributes.put(HTTPClient.REQ_HEADER_CONTENT_TYPE, HTTPClient.REQ_HEADER_CONTENT_TYPE_VALUE);
    }
    private final HTTPResponse httpResponse = mock(HTTPResponse.class);

    @Before
    public void init() throws InitializationException, IOException {
        // Mock a http client
        when(httpResponse.getAttributes()).thenReturn(responseAttributes);
        when(httpResponse.getAttribute(HTTPClient.ATT_RES_BODY)).thenReturn(responseAttributes.get(HTTPClient.ATT_RES_BODY));

        HTTPClient httpClient = mock(HTTPClient.class);
        when(httpClient.post(any(URL.class), anyString())).thenReturn(httpResponse);

        // Hook the mocked client to the processor
        PostHTTPWithJsonBody postHTTPWithJsonBody = new PostHTTPWithJsonBody(httpClient);

        // Create a test setup
        testRunner = TestRunners.newTestRunner(postHTTPWithJsonBody);

        // Mock SSL service
        SSLContext sslContext = mock(SSLContext.class);
        SSLContextService sslContextService = mock(SSLContextService.class);
        when(sslContextService.getIdentifier()).thenReturn("ssl-service");
        when(sslContextService.createSSLContext(any(SSLContextService.ClientAuth.class))).thenReturn(sslContext);

        // Add and enable the SSL service
        testRunner.addControllerService("ssl-service", sslContextService);
        testRunner.enableControllerService(sslContextService);

        // Set properties
        testRunner.setProperty(PostHTTPWithJsonBody.PROP_URL, "https://post-http-with-json-body.test");
        testRunner.setProperty(PostHTTPWithJsonBody.PROP_BODY, "{\"message\":\"hello world\"}");

        // Verify all has been set correctly
        testRunner.assertValid();
    }

    @Test
    public void singleCallOnSuccess() {
        // Create a setup for success
        when(httpResponse.getStatus()).thenReturn(HttpURLConnection.HTTP_CREATED);

        // Call onTrigger() exactly once
        testRunner.run();

        // Verify the response is handled as successful, exactly one flow file is expected
        testRunner.assertAllFlowFilesTransferred(PostHTTPWithJsonBody.REL_SUCCESS, 1);

        // Fetch the flow file
        MockFlowFile flowFile = getMockFlowFile(PostHTTPWithJsonBody.REL_SUCCESS);

        // Verify that all response attributes has been transferred
        responseAttributes.entrySet().forEach(entry -> flowFile.assertAttributeEquals(entry.getKey(), entry.getValue()));

        // Verify that response body has also been passed as content
        flowFile.assertContentEquals(responseAttributes.get(HTTPClient.ATT_RES_BODY));
    }

    private MockFlowFile getMockFlowFile(Relationship relationship) {
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(relationship);
        assertNotNull(flowFiles);
        assertTrue(flowFiles.size() == 1);

        MockFlowFile flowFile = flowFiles.get(0);
        assertNotNull(flowFile);
        return flowFile;
    }

    @Test
    public void singleCallOnFailure() {
        // Create a setup for failure
        when(httpResponse.getStatus()).thenReturn(HttpURLConnection.HTTP_INTERNAL_ERROR);

        // Set the expected error message
        String errorMessage = "Internal server error";
        when(httpResponse.getMessage()).thenReturn(errorMessage);

        // Call onTrigger() exactly once
        testRunner.run();

        // Verify the response is handled as failed, exactly one flow file is expected
        testRunner.assertAllFlowFilesTransferred(PostHTTPWithJsonBody.REL_FAILURE, 1);

        // Verify the error message has been captured
        MockFlowFile flowFile = getMockFlowFile(PostHTTPWithJsonBody.REL_FAILURE);
        flowFile.assertAttributeEquals(PostHTTPWithJsonBody.ATT_ERROR_MESSAGE, errorMessage);
    }

}
