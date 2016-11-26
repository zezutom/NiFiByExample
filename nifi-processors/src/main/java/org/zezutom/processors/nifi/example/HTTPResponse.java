package org.zezutom.processors.nifi.example;

import java.util.Collections;
import java.util.Map;

public class HTTPResponse {

    private final Map<String, String> attributes;

    private final int status;

    private final String message;

    protected HTTPResponse(Map<String, String> attributes, String message, int status) {
        this.attributes = attributes;
        this.message = message;
        this.status = status;
    }

    public Map<String, String> getAttributes() {
        return Collections.unmodifiableMap(attributes);
    }

    public String getAttribute(String name) {
        return (attributes == null) ? null : attributes.get(name);
    }

    public String getMessage() {
        return message;
    }

    public int getStatus() {
        return status;
    }
}
