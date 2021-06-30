package org.onedatashare.transferservice.odstransferservice.service.step.box;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxFileUploadSession;
import com.box.sdk.BoxFileUploadSessionPart;
import com.box.sdk.BoxFolder;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Base64;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;

public class BoxWriter implements ItemWriter<DataChunk> {

    OAuthEndpointCredential credential;
    int chunkSize;
    private BoxAPIConnection boxAPIConnection;
    EntityInfo fileInfo;
    HashMap<String, BoxFileUploadSession> fileMap;
    HashMap<String, MessageDigest> digestMap;
    List<BoxFileUploadSessionPart> parts;
    String destinationBasePath;

    public BoxWriter(OAuthEndpointCredential oauthDestCredential, EntityInfo fileInfo, int chunkSize) {
        this.credential = oauthDestCredential;
        this.boxAPIConnection = new BoxAPIConnection(credential.getToken());
        this.fileInfo = fileInfo;
        this.fileMap = new HashMap<>();
        this.chunkSize = chunkSize;
        this.digestMap = new HashMap<>();
        this.parts = new ArrayList<>();
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution){
        this.destinationBasePath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        BoxFolder boxFolder = new BoxFolder(this.boxAPIConnection, this.destinationBasePath);
        if(!this.fileMap.containsKey(this.fileInfo.getId())){
            this.fileMap.put(this.fileInfo.getId(), boxFolder.createUploadSession(this.fileInfo.getId(), this.chunkSize).getResource());
            try {
                this.digestMap.put(this.fileInfo.getId(), MessageDigest.getInstance("SHA1"));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

        }
    }

    @AfterStep
    public void afterStep(){
        BoxFileUploadSession session = this.fileMap.get(this.fileInfo.getId());
        MessageDigest messageDigest = this.digestMap.get(this.fileInfo.getId());
        session.commit(Base64.getEncoder().encodeToString(messageDigest.digest()), this.parts,null, null, null);
    }

    @Override
    public void write(List<? extends DataChunk> items) throws Exception {
        BoxFileUploadSession session = this.fileMap.get(this.fileInfo.getId());
        for(DataChunk dataChunk : items){
            BoxFileUploadSessionPart boxFileUploadSessionPart = session.uploadPart(dataChunk.getData(), dataChunk.getStartPosition(), Long.valueOf(dataChunk.getSize()).intValue(), this.fileInfo.getSize());
            this.digestMap.get(this.fileInfo.getId()).update(dataChunk.getData());
            this.parts.add(boxFileUploadSessionPart);
        }
    }
}
