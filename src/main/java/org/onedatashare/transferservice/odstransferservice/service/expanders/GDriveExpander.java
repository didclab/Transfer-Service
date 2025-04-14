package org.onedatashare.transferservice.odstransferservice.service.expanders;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.credential.EndpointCredential;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.DestinationChunkSize;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

@Service
public class GDriveExpander extends DestinationChunkSize implements FileExpander {

    @Value("${gdrive.client.id}")
    private String gDriveClientId;

    @Value("${gdrive.client.secret}")
    private String gDriveClientSecret;

    @Value("${gdrive.appname}")
    private String gdriveAppName;

    private Drive client;

    @Override
    public void createClient(EndpointCredential credential) {
        OAuthEndpointCredential oauthCred = EndpointCredential.getOAuthCredential(credential);
        try {
            NetHttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
            GoogleCredential credential1 = new GoogleCredential.Builder().setJsonFactory(GsonFactory.getDefaultInstance())
                    .setClientSecrets(gDriveClientId, gDriveClientSecret)
                    .setTransport(transport).build();
            credential1.setAccessToken(oauthCred.getToken());
            credential1.setRefreshToken(oauthCred.getRefreshToken());
            this.client = new Drive.Builder(transport, GsonFactory.getDefaultInstance(), credential1)
                    .setApplicationName(gdriveAppName)
                    .build();
        } catch (GeneralSecurityException | IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<EntityInfo> expandedFileSystem(List<EntityInfo> userSelectedResources, String basePath) {
        Stack<File> fileListStack = new Stack<>();
        List<EntityInfo> fileInfoList = new ArrayList<>();
        if(userSelectedResources.isEmpty()){
            googleDriveLister(fileListStack, fileInfoList, "", "");
        }else{
            for(EntityInfo fileInfo : userSelectedResources){
                String fileQuery = "'" + fileInfo.getId() + "' in parents and trashed=false";
                googleDriveLister(fileListStack, fileInfoList, fileQuery, fileInfo.getId());
            }
        }
        while(!fileListStack.isEmpty()){
            File file = fileListStack.pop();
            String fileQuery = "'" + file.getId() + "' in parents and mimeType != 'application/vnd.google-apps.folder' and trashed=false";
            googleDriveLister(fileListStack, fileInfoList, fileQuery, file.getId());
        }
        return fileInfoList;
    }

    private void googleDriveLister(Stack<File> fileListStack, List<EntityInfo> fileInfoList, String fileQuery, String parentFolderId) {
        FileList fileList;
        String pageToken = "";
        try {
            do {
                fileList = this.client.files().list()
                        .setQ(fileQuery)
                        .setFields("nextPageToken, files(id, name, parents, size, mimeType)")
                        .setPageToken(pageToken)
                        .execute();
                for(File file : fileList.getFiles()){
                    if(file.getId().equals(parentFolderId)){
                        continue;
                    }
                    if(file.getMimeType().equals("application/vnd.google-apps.folder")){
                        fileListStack.add(file);
                    }else{
                        fileInfoList.add(googleFileToEntityInfo(file));
                    }
                }
                pageToken = fileList.getNextPageToken();
            } while (pageToken != null);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private EntityInfo googleFileToEntityInfo(File googleFile){
        EntityInfo entityInfo = new EntityInfo();
        entityInfo.setId(googleFile.getId());
        entityInfo.setSize(googleFile.getSize());
        entityInfo.setPath(String.valueOf(googleFile.getParents()));
        return entityInfo;
    }
}
