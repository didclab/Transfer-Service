package org.onedatashare.transferservice.odstransferservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class AuthenticationService {
    private static final String CLIENT_ID = System.getenv("OAUTH_CLIENT_ID");
    private static final String CLIENT_SECRET = System.getenv("OAUTH_CLIENT_SECRET");

    public Map<String, Object> startDeviceAuthentication() throws Exception {
        Map<String, Object> tokenResponsePayload = new HashMap<>();
        String SCOPE = "openid profile email org.cilogon.userinfo";
        String HOST = "cilogon.org";

        String DEVICE_ENDPOINT = String.format("https://%s/oauth2/device_authorization", HOST);
        String TOKEN_ENDPOINT = String.format("https://%s/oauth2/token", HOST);

        HttpClient httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .build();
        ObjectMapper objectMapper = new ObjectMapper();

        //DEVICE REQUEST
        Map<String, Object> deviceRequestPayload = new HashMap<>();
        deviceRequestPayload.put("client_id", CLIENT_ID);
        deviceRequestPayload.put("client_secret", CLIENT_SECRET);
        deviceRequestPayload.put("scope", SCOPE);

        HttpRequest deviceRequest = HttpRequest.newBuilder()
                .POST(buildFormDataFromMap(deviceRequestPayload))
                .uri(URI.create(DEVICE_ENDPOINT))
                .setHeader("User-Agent", "Java HttpClient Bot") // add request header
                .header("Content-Type", "application/x-www-form-urlencoded")
                .build();
        HttpResponse<String> deviceResponse = httpClient.send(deviceRequest, HttpResponse.BodyHandlers.ofString());
        Map<String, Object> deviceResponsePayload = objectMapper.readValue(deviceResponse.body(), HashMap.class);

        //DEVICE RESPONSE CHECK
        if (deviceResponsePayload.containsKey("error")) {
            String errorDescription = deviceResponsePayload.get("error_description").toString();
            if (errorDescription.length() > 0) {
                System.out.format("Error description : %s", deviceResponsePayload.get("error_description"));
            }
            throw new Exception(errorDescription);
        }
        String DEVICE_CODE = deviceResponsePayload.get("device_code").toString();
        String USER_CODE = deviceResponsePayload.get("user_code").toString();
        String VERIFICATION_URI_COMPLETE = deviceResponsePayload.get("verification_uri_complete").toString();
        String VERIFICATION_URI = deviceResponsePayload.get("verification_uri").toString();
        int INTERVAL = 5;
        if (deviceResponsePayload.containsKey("interval")) {
            INTERVAL = Integer.parseInt(deviceResponsePayload.get("interval").toString());
        }

        boolean checks_failed = false;
        if (DEVICE_CODE.length() == 0) {
            System.out.println("Error: No device code found in response");
            checks_failed = true;
        }
        if (USER_CODE.length() == 0) {
            System.out.println("Error: No user code found in response");
            checks_failed = true;
        }
        if (checks_failed) {
            throw new Exception("device code or user code not found in response");
        }

        //USER MESSAGE
        System.out.println("Device code successful");
        System.out.format("On your computer or mobile device navigate to: %s \n", VERIFICATION_URI_COMPLETE);
        System.out.format("OR \n Navigate to %s and  enter the following code: %s \n", VERIFICATION_URI, USER_CODE);


        //TOKEN REQUEST
        String GRANT_TYPE = "urn:ietf:params:oauth:grant-type:device_code";
        Map<String, Object> tokenRequestPayload = new HashMap<>();
        tokenRequestPayload.put("client_id", CLIENT_ID);
        tokenRequestPayload.put("client_secret", CLIENT_SECRET);
        tokenRequestPayload.put("device_code", DEVICE_CODE);
        tokenRequestPayload.put("grant_type", GRANT_TYPE);


        boolean isAuthenticated = false;

        //WAIT FOR AUTH TOKENS
        while (!isAuthenticated) {
            System.out.println("Checking if user completed the flow");
            HttpRequest tokenRequest = HttpRequest.newBuilder()
                    .POST(buildFormDataFromMap(tokenRequestPayload))
                    .uri(URI.create(TOKEN_ENDPOINT))
                    .setHeader("User-Agent", "Java HttpClient") // add request header
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .build();
            HttpResponse<String> tokenResponse = httpClient.send(tokenRequest, HttpResponse.BodyHandlers.ofString());
            tokenResponsePayload = objectMapper.readValue(tokenResponse.body(), HashMap.class);
            if (tokenResponsePayload.containsKey("error")) {
                String error = tokenResponsePayload.get("error").toString();
                String error_description = tokenResponsePayload.get("error_description").toString();
                System.out.println(error_description);
                if (error.equals("authorization_pending") || error.equals("slow_down")) {
                    if (error.equals("slow_down")) {
                        INTERVAL += 5;
                    }
                    Thread.sleep(INTERVAL * 1000);
                } else {
                    if (error_description.equals("no pending request")) {
                        System.out.println("Error: User denied user_code. \n Please begin a new device code request");
                    } else {
                        System.out.format("Error %s \n", error_description);
                        if (error_description.equals("device code expired")) {
                            System.out.println("Please begin new device code request");
                        }
                    }
                    throw new Exception(error_description);
                }
            } else {
                isAuthenticated = true;
                break;
            }
        }
        return tokenResponsePayload;
    }

    private HttpRequest.BodyPublisher buildFormDataFromMap(Map<String, Object> data) {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            if (builder.length() > 0) {
                builder.append("&");
            }
            builder.append(URLEncoder.encode(entry.getKey().toString(), StandardCharsets.UTF_8));
            builder.append("=");
            builder.append(URLEncoder.encode(entry.getValue().toString(), StandardCharsets.UTF_8));
        }
        return HttpRequest.BodyPublishers.ofString(builder.toString());
    }
}
