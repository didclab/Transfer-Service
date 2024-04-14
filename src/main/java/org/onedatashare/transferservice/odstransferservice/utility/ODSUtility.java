package org.onedatashare.transferservice.odstransferservice.utility;

import com.amazonaws.services.s3.model.UploadPartRequest;
import com.dropbox.core.DbxRequestConfig;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.onedatashare.commonservice.model.credential.EndpointCredentialType;
import com.onedatashare.commonservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.HashSet;

public class ODSUtility {

    private static String odsClientID = "OneDataShare-DIDCLab";

    private static String gDriveClientId= System.getenv("ODS_GDRIVE_CLIENT_ID");

    private static String gDriveClientSecret = System.getenv("ODS_GDRIVE_CLIENT_SECRET");

//    @Value("${gdrive.appname}")
//    private static String gdriveAppName;

    public static DbxRequestConfig dbxRequestConfig = DbxRequestConfig.newBuilder(odsClientID).build();

    public static DataChunk makeChunk(int size, byte[] data, int startPosition, int chunkIdx, String fileName) {
        DataChunk dataChunk = new DataChunk();
        dataChunk.setStartPosition(startPosition);
        dataChunk.setChunkIdx(chunkIdx);
        dataChunk.setFileName(fileName);
        dataChunk.setData(data);
        dataChunk.setSize(size);
        return dataChunk;
    }
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
        System.out.println(gDriveClientId);
        System.out.println(gDriveClientSecret);
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
        for(File file : files.getFiles()){
            if(file.getId().equals(basePath)){
                return file;
            }
        }
        File fileMetadata = new File();
        File ret= new File();
        String[] path = basePath.split("/");
        for(String mini: path){
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
        uploadPartRequest.setPartNumber(dataChunk.getChunkIdx()+1); //by default we start from chunks 0-N but AWS SDK must have 1-10000 so we just add 1
        uploadPartRequest.setPartSize(dataChunk.getSize());
        return uploadPartRequest;
    }

    public static final EndpointCredentialType[] SEEKABLE_PROTOCOLS = new EndpointCredentialType[]{EndpointCredentialType.s3, EndpointCredentialType.vfs, EndpointCredentialType.http, EndpointCredentialType.box};

    public static final HashSet<EndpointCredentialType> fullyOptimizableProtocols = new HashSet<EndpointCredentialType>(Arrays.asList(SEEKABLE_PROTOCOLS));
}
