package org.greenplum.pxf.automation.utils.curl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class to interact with curl
 */
public class CurlUtils {
    private final String protocol = "http";
    private final String defaultRequest = "GET";
    private String command;

    public CurlUtils(String host, String port, String path) {
        Map<String, String> headers = new HashMap<>();
        headers.put("X-GP-USER", "hacker");
        formCommand(host, port, path, defaultRequest, headers, null);
    }
    public CurlUtils(String host, String port, String path, String requestType, Map<String, String> headers, List<String> params) {
        formCommand(host, port, path, requestType, headers, params);
    }

    public String getCommand() {
        return command;
    }

    private void formCommand(String host, String port, String path, String requestType, Map<String, String> headers, List<String> params) {
        StringBuilder str = new StringBuilder();
        str.append("curl -X " + requestType);
        str.append(" \"" + protocol + "://" + host + ":" + port + "/" + path + "\" ");

        if (headers != null && !headers.isEmpty()) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                str.append("--header \"" + entry.getKey() + "=" + entry.getValue() + "\" ");
            }
        }

        if (params != null && !params.isEmpty()) {
            for (String param: params) {
                str.append("--data \"" + param + "\" ");
            }
        }
        command = str.toString();
    }

}
