package org.onedatashare.transferservice.odstransferservice.service.step.googleDrive;

import com.box.sdk.BoxUser;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.About;
import com.google.api.services.drive.model.File;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.AuthenticateCredentials;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import java.io.ByteArrayOutputStream;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.OWNER_ID;

public class GDriveReader extends AbstractItemCountingItemStreamItemReader<DataChunk> {

    Logger logger = LoggerFactory.getLogger(GDriveReader.class);
    private final EntityInfo fileInfo;
    private OAuthEndpointCredential credential;
    private final FilePartitioner partitioner;
    private Drive client;
    private String fileName;
    private File file;

    String usersEmail;
    String ownerId;

    private AuthenticateCredentials authenticateCredentials;

    public GDriveReader(OAuthEndpointCredential credential, EntityInfo fileInfo,AuthenticateCredentials authenticateCredentials){
        this.credential = credential;
        this.fileInfo = fileInfo;
        this.partitioner = new FilePartitioner(fileInfo.getChunkSize());
        this.setName(ClassUtils.getShortName(GDriveReader.class));
        logger.info(credential.toString());
        this.authenticateCredentials = authenticateCredentials;
    }

    @Override
    protected DataChunk doRead() throws Exception {
        if(this.credential.isTokenExpires()){
            this.credential = authenticateCredentials.checkExpiryAndGenerateNew( usersEmail, ODSConstants.GOOGLEDRIVE, ownerId);
            doOpen();
        }
        FilePart filePart = this.partitioner.nextPart();
        if(filePart == null || filePart.getSize() == 0) return null;
        Drive.Files.Get get = this.client.files().get(fileInfo.getId());
        get.getMediaHttpDownloader().setChunkSize(this.fileInfo.getChunkSize()).setContentRange(filePart.getStart(), filePart.getEnd());
        logger.info(get.toString());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(filePart.getSize());
        get.executeMediaAndDownloadTo(outputStream);
        DataChunk chunk = ODSUtility.makeChunk(filePart.getSize(), outputStream.toByteArray(), (int) filePart.getStart(), (int) filePart.getPartIdx(), this.fileName);
        outputStream.close();
        logger.info(chunk.toString());
        return chunk;
    }

    @Override
    protected void doOpen() throws Exception {
        this.client = ODSUtility.authenticateDriveClient(this.credential);
        this.file = this.client.files().get(fileInfo.getId()).setFields("id, name, kind, mimeType, size, modifiedTime").execute();
        this.fileName = file.getName();
        this.partitioner.createParts(this.fileInfo.getSize(), this.fileName);
        logger.info(file.toString());
        About about = client.about().get().execute();
         this.usersEmail = about.getUser().getEmailAddress();
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution){
        this.ownerId = stepExecution.getJobParameters().getString(OWNER_ID);
    }

    @Override
    protected void doClose() {
        this.client = null;
    }
}