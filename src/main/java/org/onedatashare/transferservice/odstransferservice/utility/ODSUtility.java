package org.onedatashare.transferservice.odstransferservice.utility;

import com.amazonaws.services.s3.model.UploadPartRequest;
import com.dropbox.core.DbxRequestConfig;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.model.credential.EndpointCredential;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.HashSet;

public class ODSUtility {

    private static String odsClientID = "OneDataShare-DIDCLab";

    //    @Value("${gdrive.client.id}")
    private static String gDriveClientId = System.getenv("ODS_GDRIVE_CLIENT_ID");

    //    @Value("${gdrive.client.secret}")
    private static String gDriveClientSecret = System.getenv("ODS_GDRIVE_CLIENT_SECRET");

//    @Value("${gdrive.appname}")
//    private static String gdriveAppName;

    public static DbxRequestConfig dbxRequestConfig = DbxRequestConfig.newBuilder(odsClientID).build();

    public static DataChunk makeChunk(long size, byte[] data, long startPosition, int chunkIdx, String fileName) {
        DataChunk dataChunk = new DataChunk();
        dataChunk.setStartPosition(startPosition);
        dataChunk.setChunkIdx(chunkIdx);
        dataChunk.setFileName(fileName);
        dataChunk.setData(data);
        dataChunk.setSize(size);
        return dataChunk;
    }

    public static Drive authenticateDriveClient(OAuthEndpointCredential oauthCred) throws GeneralSecurityException, IOException {
        GoogleCredential credential1 = new GoogleCredential.Builder().setJsonFactory(GsonFactory.getDefaultInstance())
                .setClientSecrets(gDriveClientId, gDriveClientSecret)
                .setTransport(GoogleNetHttpTransport.newTrustedTransport()).build();
        credential1.setAccessToken(oauthCred.getToken());
        credential1.setRefreshToken(oauthCred.getRefreshToken());
        return new Drive.Builder(GoogleNetHttpTransport.newTrustedTransport(), GsonFactory.getDefaultInstance(), credential1)
                .setApplicationName("OneDataShare-Prod")
                .build();
    }

    public static File gdriveMakeDir(String basePath, Drive client) throws IOException {
        Drive.Files.List request = client.files().list().setQ(
                        "mimeType='application/vnd.google-apps.folder' and trashed=false")
                .setFields("nextPageToken, files(id,name)")
                .setSpaces("drive");
        FileList files = request.execute();
        for (File file : files.getFiles()) {
            if (file.getId().equals(basePath)) {
                return file;
            }
        }
        File fileMetadata = new File();
        File ret = new File();
        String[] path = basePath.split("/");
        for (String mini : path) {
            fileMetadata.setName(mini);
            fileMetadata.setMimeType("application/vnd.google-apps.folder");
            ret = client.files().create(fileMetadata)
                    .setFields("id")
                    .execute();
        }
        return ret;
    }


    public static UploadPartRequest makePartRequest(DataChunk dataChunk, String bucketName, String uploadId, String key, boolean lastPart) {
        UploadPartRequest uploadPartRequest = new UploadPartRequest();
        uploadPartRequest.setInputStream(new ByteArrayInputStream(dataChunk.getData()));
        uploadPartRequest.setBucketName(bucketName);
        uploadPartRequest.withLastPart(lastPart);
        uploadPartRequest.setUploadId(uploadId);
        uploadPartRequest.setKey(key);
//        uploadPartRequest.setFileOffset(dataChunk.getStartPosition());
        uploadPartRequest.setPartNumber(dataChunk.getChunkIdx() + 1); //by default we start from chunks 0-N but AWS SDK must have 1-10000 so we just add 1
        uploadPartRequest.setPartSize(dataChunk.getSize());
        return uploadPartRequest;
    }

    public static final EndpointType[] SEEKABLE_PROTOCOLS = new EndpointType[]{EndpointType.s3, EndpointType.vfs, EndpointType.http, EndpointType.box};

    public static final HashSet<EndpointType> fullyOptimizableProtocols = new HashSet<EndpointType>(Arrays.asList(SEEKABLE_PROTOCOLS));

    public static String uriFromEndpointCredential(EndpointCredential credential, EndpointType type) {
        AccountEndpointCredential ac;
        switch (type) {
            case ftp:
            case sftp:
            case scp:
            case http:
                ac = (AccountEndpointCredential) credential;
                URI uri = URI.create(ac.getUri());
                return uri.getHost();
            case s3:
                ac = (AccountEndpointCredential) credential;
                URI s3Uri = URI.create(constructS3URI(ac.getUri(), ""));
                return s3Uri.getHost();
            case box:
                return "box.com";
            case dropbox:
                return "dropbox.com";
            case gdrive:
                return "drive.google.com";
            default:
                return "";
        }
    }

    public static String constructS3URI(String uri, String fileKey) {
        StringBuilder builder = new StringBuilder();
        String[] temp = uri.split(":::");
        String bucketName = temp[1];
        String region = temp[0];
        builder.append("https://").append(bucketName).append(".").append("s3.").append(region).append(".").append("amazonaws.com/").append(fileKey);
        return builder.toString();
    }
}
