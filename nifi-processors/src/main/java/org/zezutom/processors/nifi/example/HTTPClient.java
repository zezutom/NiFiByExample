package org.zezutom.processors.nifi.example;

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HTTPClient {

    private static final String PROTOCOL_HTTPS = "https";

    /** Request Properties **/

    public static final String REQ_HEADER_USER_AGENT = "User-Agent";

    public static final String REQ_HEADER_USER_AGENT_VALUE = "Mozilla/5.0";

    public static final String REQ_HEADER_CONTENT_TYPE = "Content-Type";

    public static final String REQ_HEADER_CONTENT_TYPE_VALUE = "application/json; charset=UTF-8";

    public static final String REQ_HEADER_CONTENT_LENGTH = "Content-Length";

    public static final String REQ_HEADER_ACCEPT = "Accept";

    public static final String REQ_HEADER_ACCEPT_VALUE = "application/json";

    public static final String REQ_METHOD_POST = "POST";


    /** Response Attributes **/
    public static final String ATT_RES_STATUS_CODE = "status.code";

    public static final String ATT_RES_STATUS_MESSAGE = "status.message";

    public static final String ATT_RES_BODY = "response.body";

    public static final String ATT_RES_HOST_URL = "host.url";


    /** Logging **/
    public static final String REQ_LOG_FORMAT = "Method: '%s' | URL: '%s' | Headers: %s | Body: '%s'";

    public static final String HEADER_LOG_FORMAT = "%s: %s";

    public static final String RECORD_DELIMITER = ",";

    private SSLContextService sslContextService;

    private ComponentLog logger;

    HTTPClient(SSLContextService sslContextService, ComponentLog logger) {
        this.sslContextService = sslContextService;
        this.logger = logger;
    }

    public URL parseURL(PropertyValue propertyValue, FlowFile flowFile) throws MalformedURLException {
        // Read url from the context, resolve placeholders
        String resolvedURL = propertyValue.evaluateAttributeExpressions(flowFile).getValue().trim();
        return new URL(resolvedURL);
    }

    public HTTPResponse post(URL url, String json) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        if (PROTOCOL_HTTPS.equalsIgnoreCase(url.getProtocol())) {
            HttpsURLConnection sslConnection = (HttpsURLConnection) connection;
            SSLContext sslContext = (sslContextService == null) ? null : sslContextService.createSSLContext(SSLContextService.ClientAuth.NONE);

            if (sslContext != null) {
                sslConnection.setSSLSocketFactory(sslContext.getSocketFactory());
            }
        }
        connection.setRequestMethod(REQ_METHOD_POST);

        // Good to know: without setting the agent the server won't accept the request (http status 403)
        connection.setRequestProperty(REQ_HEADER_USER_AGENT, REQ_HEADER_USER_AGENT_VALUE);

        connection.setRequestProperty(REQ_HEADER_CONTENT_TYPE, REQ_HEADER_CONTENT_TYPE_VALUE);
        connection.setRequestProperty(REQ_HEADER_CONTENT_LENGTH, "" + json.getBytes().length);
        connection.setRequestProperty(REQ_HEADER_ACCEPT, REQ_HEADER_ACCEPT_VALUE);
        connection.setDoOutput(true);
        connection.setDoInput(true);

        try (OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream(), Charset.forName("UTF-8"))) {
            writer.write(json);
            writer.flush();
        }

        // Log the request (debug only)
        logger.debug(String.format(REQ_LOG_FORMAT,
                connection.getRequestMethod(),
                connection.getURL().toString(),
                flattenHeaders(connection),
                json));

        return new HTTPResponse(
                parseResponseAttributes(connection),
                connection.getResponseMessage(),
                connection.getResponseCode());
    }

    private String flattenHeaders(HttpURLConnection connection) {
        List<String> headers = connection.getHeaderFields().entrySet().stream()
                .map(entry ->
                        String.format(HEADER_LOG_FORMAT, entry.getKey(),
                                String.join(RECORD_DELIMITER, entry.getValue())))
                .collect(Collectors.toList());
        return String.join(RECORD_DELIMITER, headers);
    }

    private Map<String, String> parseResponseAttributes(HttpURLConnection connection) throws IOException {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(ATT_RES_STATUS_CODE, String.valueOf(connection.getResponseCode()));
        attributes.put(ATT_RES_STATUS_MESSAGE, connection.getResponseMessage());
        attributes.put(ATT_RES_HOST_URL, connection.getURL().getHost());
        attributes.put(CoreAttributes.MIME_TYPE.key(), connection.getContentType());
        attributes.put(ATT_RES_BODY, parseResponseBody(connection));
        return attributes;
    }

    private String parseResponseBody(HttpURLConnection connection) throws IOException {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String inputLine;
            StringBuilder sb = new StringBuilder();
            while ((inputLine = br.readLine()) != null) {
                sb.append(inputLine);
            }
            return sb.toString();
        }
    }
}
