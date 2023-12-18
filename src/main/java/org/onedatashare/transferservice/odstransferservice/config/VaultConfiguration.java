package org.onedatashare.transferservice.odstransferservice.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.jopenlibs.vault.Vault;
import io.github.jopenlibs.vault.VaultConfig;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;


import org.onedatashare.transferservice.odstransferservice.service.AuthenticationService;

public class VaultConfiguration {
    private static Vault vaultServiceInstance;
    private static String vaultServerAddress = System.getenv("VAULT_URI");
    private static String secretsPath = System.getenv("VAULT_SECRETS_PATH");
    private static String shouldLoadSecrets = System.getenv("VAULT_LOAD_SECRETS");
    private static String vaultTransferServiceUserRole = System.getenv("VAULT_TRANSFER_SERVICE_USER_ROLE");
    private VaultConfiguration() {

    }

    public static void loadSecrets() {
        try {

            if (shouldLoadSecrets == null || !shouldLoadSecrets.equals("true")) {
                return;
            }
            Vault vaultServiceInstance = getInstance();
            var secrets = vaultServiceInstance.logical().read(secretsPath).getData();
            for (Map.Entry<String, String> kv : secrets.entrySet()) {
                System.setProperty(kv.getKey(), kv.getValue());
            }
        } catch (Exception e) {
            System.out.println("Exception while loading secrets from vault:" + e.getMessage());
        }
    }
    
    public static Vault getInstance() throws Exception {

        if (vaultServiceInstance == null) {
            AuthenticationService authenticator = new AuthenticationService();
            Map<String, Object> authTokens = authenticator.startDeviceAuthentication();
            setOdsUserFromToken(authTokens.get("id_token").toString());
            return authenticateVaultWithIdToken(authTokens.get("id_token").toString());

        }
        return vaultServiceInstance;
    }

    private static void setOdsUserFromToken(String idToken) throws Exception {
        String payload = idToken.split("\\.")[1];
        String decodedPayload = new String(Base64.getDecoder().decode(payload));
        ObjectMapper objectMapper = new ObjectMapper();
        HashMap<String, Object> decodedPayloadMap = objectMapper.readValue(decodedPayload, HashMap.class);
        System.setProperty("ods.user", decodedPayloadMap.get("email").toString());
    }

    private static Vault authenticateVaultWithIdToken(String idToken) throws Exception {


        String jwtLoginUri = vaultServerAddress + "/v1/auth/jwt/login";
        HttpClient httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .build();
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> vaultAuthPayload = new HashMap<>();
        vaultAuthPayload.put("jwt", idToken);
        vaultAuthPayload.put("role", vaultTransferServiceUserRole);
        HttpRequest vaultAuthrequest = HttpRequest.newBuilder()
                .POST(buildFormDataFromMap(vaultAuthPayload))
                .uri(URI.create(jwtLoginUri))
                .setHeader("User-Agent", "Java HttpClient Bot") // add request header
                .header("Content-Type", "application/x-www-form-urlencoded")
                .build();

        HttpResponse<String> vaultAuthresponse = httpClient.send(vaultAuthrequest, HttpResponse.BodyHandlers.ofString());
        HashMap<String, Object> vaultAuthRespnseMap = objectMapper.readValue(vaultAuthresponse.body(), HashMap.class);
        HashMap<Object, Object> vaultAuthData = (HashMap<Object, Object>) vaultAuthRespnseMap.get("auth");
        String vaultToken = vaultAuthData.get("client_token").toString();

        // Configure the Vault client with the server address
        VaultConfig config = new VaultConfig().address(vaultServerAddress).token(vaultToken).engineVersion(2).build();
        vaultServiceInstance = Vault.create(config);
        return vaultServiceInstance;
    }

    private static HttpRequest.BodyPublisher buildFormDataFromMap(Map<String, Object> data) {
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
