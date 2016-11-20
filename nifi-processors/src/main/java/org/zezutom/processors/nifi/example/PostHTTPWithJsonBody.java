/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zezutom.processors.nifi.example;

import com.jayway.jsonpath.JsonPath;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
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
import java.util.*;
import java.util.stream.Collectors;

@Tags({"http", "https", "remote", "json"})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@CapabilityDescription("Performs an HTTP Post with the specified JSON content.")
public class PostHTTPWithJsonBody extends AbstractProcessor {

    public static final String PROTOCOL_HTTPS = "https";

    /** Logging **/
    public static final String REQ_LOG_FORMAT = "Method: '%s' | URL: '%s' | Headers: %s | Body: '%s'";

    public static final String HEADER_LOG_FORMAT = "%s: %s";

    public static final String RECORD_DELIMITER = ",";

    /** Request Properties **/

    public static final String REQ_HEADER_USER_AGENT = "User-Agent";

    public static final String REQ_HEADER_USER_AGENT_VALUE = "Mozilla/5.0";

    public static final String REQ_HEADER_CONTENT_TYPE = "Content-Type";

    public static final String REQ_HEADER_CONTENT_TYPE_VALUE = "application/json; charset=UTF-8";

    public static final String REQ_HEADER_CONTENT_LENGTH = "Content-Length";

    public static final String REQ_HEADER_ACCEPT = "Accept";

    public static final String REQ_HEADER_ACCEPT_VALUE = "application/json";

    public static final String REQ_METHOD_POST = "POST";


    /** Response Properties **/

    public static final String RES_STATUS_CODE = "status.code";

    public static final String RES_STATUS_MESSAGE = "status.message";

    public static final String RES_BODY = "response.body";

    public static final String RES_HOST_URL = "host.url";


    /** Processor Properties and Relationships **/

    public static final PropertyDescriptor PROP_URL = new PropertyDescriptor
            .Builder().name("URL")
            .description("The URL to post to, including scheme, host, port and path.")
            .expressionLanguageSupported(true)
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_BODY = new PropertyDescriptor
            .Builder().name("Request Body")
            .description("Must be a valid JSON.")
            .expressionLanguageSupported(true)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL (https) connections.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Status code 2xx.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("Status codes 4xx and 5xx.")
            .build();


    private ComponentLog logger;

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PROP_URL);
        descriptors.add(PROP_BODY);
        descriptors.add(PROP_SSL_CONTEXT_SERVICE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);

        logger = context.getLogger();
        if (logger == null) {
            throw new IllegalStateException("Logger can't be null!");
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        // Get or create a request flow file
        FlowFile flowFile = session.get();
        if ( flowFile == null ) flowFile = session.create();

        try {
            // Parse the URL to post to and a request body
            URL url = parseURL(context, flowFile);
            String json = parseJsonBody(context, flowFile);

            // Make a POST request
            HttpURLConnection connection = httpPost(context, url, json);

            // Capture response properties and headers as attributes
            Map<String, String> attributes = parseResponseAttributes(connection);
            flowFile = session.putAllAttributes(flowFile, attributes);

            // Capture response body
            String resBody = attributes.get(RES_BODY);
            if (resBody != null) {
                flowFile = session.write(flowFile, outputStream -> outputStream.write(resBody.getBytes()));
            }

            // Direct the flow file based on the response code
            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_CREATED || responseCode == HttpURLConnection.HTTP_OK) {
                session.transfer(flowFile, REL_SUCCESS);
            }
            else {
                logger.error(String.format("There was an error with POST request, status: %s, response message: '%s'",
                        responseCode, connection.getResponseMessage()));
                session.transfer(flowFile, REL_FAILURE);
            }
        } catch (IOException e) {
            session.transfer(flowFile, REL_FAILURE);
            throw new ProcessException("Invalid URL", e);
        }
    }

    private Map<String, String> parseResponseAttributes(HttpURLConnection connection) throws IOException {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(RES_STATUS_CODE, String.valueOf(connection.getResponseCode()));
        attributes.put(RES_STATUS_MESSAGE, connection.getResponseMessage());
        attributes.put(RES_HOST_URL, connection.getURL().getHost());
        attributes.put(CoreAttributes.MIME_TYPE.key(), connection.getContentType());
        attributes.put(RES_BODY, parseResponseBody(connection));
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

    private URL parseURL(ProcessContext context, FlowFile flowFile) throws MalformedURLException {
        // Read url from the context, resolve placeholders
        String resolvedURL = context.getProperty(PROP_URL).evaluateAttributeExpressions(flowFile).getValue().trim();
        return new URL(resolvedURL);
    }

    private HttpURLConnection httpPost(ProcessContext context, URL url, String json) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        if (PROTOCOL_HTTPS.equalsIgnoreCase(url.getProtocol())) {
            HttpsURLConnection sslConnection = (HttpsURLConnection) connection;
            SSLContextService sslContextService = context.getProperty(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
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
        return connection;
    }

    private String flattenHeaders(HttpURLConnection connection) {
        List<String> headers = connection.getHeaderFields().entrySet().stream()
                .map(entry ->
                        String.format(HEADER_LOG_FORMAT, entry.getKey(),
                        String.join(RECORD_DELIMITER, entry.getValue())))
                .collect(Collectors.toList());
        return String.join(RECORD_DELIMITER, headers);
    }

    private String parseJsonBody(ProcessContext context, FlowFile flowFile) {
        String body = context.getProperty(PROP_BODY).evaluateAttributeExpressions(flowFile).getValue().trim();
        return JsonPath.parse(body).jsonString();
    }
}
