package org.onedatashare.transferservice.odstransferservice.utility;

import lombok.Builder;
import lombok.Getter;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.pools.GDriveConnectionPool;
import org.onedatashare.transferservice.odstransferservice.pools.HttpConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpRange;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.io.OutputStream;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

@Builder
public class GDriveHelper {

    private GDriveConnectionPool connectionPool;
    @Getter
    private String sessionUri;
    private EntityInfo fileInfo;

    final private Logger logger = LoggerFactory.getLogger(GDriveHelper.class);
    private OAuthEndpointCredential credential;

    private HttpRequest initializeUploadRequest(String fileName, String parentFolderId) throws JSONException {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", fileName);
        jsonObject.put("parents", Arrays.asList(parentFolderId));
        return HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(jsonObject.toString()))
                .uri(URI.create("https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable"))
                .setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + credential.getToken())
                .setHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=UTF-8")
                .setHeader(HttpHeaders.CONTENT_LENGTH, String.format(Locale.ENGLISH, "%d", jsonObject.toString().getBytes().length))
                .setHeader("X-Upload-Content-Type", URLConnection.guessContentTypeFromName(fileName))
                .setHeader("X-Upload-Content-Length", String.valueOf(this.fileInfo.getSize()))
                .build();
    }

    public int initializeUpload(String fileName, String parentFolderId) throws InterruptedException, JSONException, IOException {
        HttpClient client = this.connectionPool.borrowObject();

        HttpResponse<String> response = client.send(initializeUploadRequest(fileName, parentFolderId), HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == HttpStatus.OK.value()) {
            this.sessionUri = response.headers().firstValue("location").get();
        }
        this.connectionPool.returnObject(client);
        return response.statusCode();
    }

    public HttpResponse uploadChunk(DataChunk chunk) throws InterruptedException, IOException, URISyntaxException {
        HttpClient client = connectionPool.borrowObject();
        HttpResponse<String> response = client.send(uploadChunkRequest(chunk), HttpResponse.BodyHandlers.ofString());
        connectionPool.returnObject(client);
        return response;
    }

    private HttpRequest uploadChunkRequest(DataChunk chunk) throws URISyntaxException {
        int to = (int)chunk.getStartPosition()+chunk.getData().length-1;
        return HttpRequest.newBuilder()
                .uri(new URI(this.sessionUri))
                .setHeader(HttpHeaders.CONTENT_LENGTH, String.format(Locale.ENGLISH, "%d", chunk.getData().length))
                .setHeader(HttpHeaders.CONTENT_RANGE, "bytes "+chunk.getStartPosition()+"-"+to+"/"+this.fileInfo.getSize())
                .PUT(HttpRequest.BodyPublishers.ofByteArray(chunk.getData()))
                .build();
    }

}
