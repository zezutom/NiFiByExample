
package org.zezutom.processors.nifi.example;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PostHTTPWithJsonBodyTest {

    private TestRunner testRunner;

    @Before
    public void init() throws InitializationException, IOException {
        // Mock a http client
        HTTPResponse success = mock(HTTPResponse.class);
        when(success.getStatus()).thenReturn(HttpURLConnection.HTTP_CREATED);

        HTTPClient httpClient = mock(HTTPClient.class);
        when(httpClient.post(any(URL.class), anyString())).thenReturn(success);

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
    public void singleCallWithValidInputIsSuccessful() {
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PostHTTPWithJsonBody.REL_SUCCESS, 1);
    }

}
