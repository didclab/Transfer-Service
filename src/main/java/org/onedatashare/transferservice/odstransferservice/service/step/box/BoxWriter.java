package org.onedatashare.transferservice.odstransferservice.service.step.box;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxFileUploadSession;
import com.box.sdk.BoxFileUploadSessionPart;
import com.box.sdk.BoxFolder;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3.AmazonS3Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;

public class BoxWriter implements ItemWriter<DataChunk> {

    private OAuthEndpointCredential credential;
    int chunkSize;
    private BoxAPIConnection boxAPIConnection;
    EntityInfo fileInfo;
    private HashMap<String, BoxFileUploadSession> fileMap;
    private HashMap<String, MessageDigest> digestMap;
    private List<BoxFileUploadSessionPart> parts;
    String destinationBasePath;
    BoxFolder boxFolder;

    Logger logger = LoggerFactory.getLogger(BoxWriter.class);


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
        this.boxFolder = new BoxFolder(this.boxAPIConnection, this.destinationBasePath);
    }

    @AfterStep
    public void afterStep(){
        BoxFileUploadSession session = this.fileMap.get(this.fileInfo.getId());
        logger.info(session.toString());
        MessageDigest messageDigest = this.digestMap.get(this.fileInfo.getId());
        logger.info(Arrays.toString(messageDigest.digest()));
        session.commit(Base64.getEncoder().encodeToString(messageDigest.digest()), this.parts,new HashMap<>(), null, null);
    }

    @Override
    public void write(List<? extends DataChunk> items) throws NoSuchAlgorithmException {
        for(DataChunk dataChunk : items){
            String fileName = dataChunk.getFileName();
            if(!this.fileMap.containsKey(fileName)){
                this.boxFolder.createUploadSession(fileName, this.fileInfo.getSize());
            }else{
                BoxFileUploadSessionPart part = this.fileMap.get(fileName).uploadPart(dataChunk.getData(), dataChunk.getStartPosition(), Long.valueOf(dataChunk.getSize()).intValue(), this.fileInfo.getSize());
                if(this.digestMap.containsKey(fileName)){
                    this.digestMap.get(fileName).update(dataChunk.getData());
                }else{
                    MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
                    this.digestMap.put(fileName, messageDigest);
                    this.digestMap.get(fileName).update(dataChunk.getData());
                }
                this.parts.add(part);
            }
        }
    }
}
