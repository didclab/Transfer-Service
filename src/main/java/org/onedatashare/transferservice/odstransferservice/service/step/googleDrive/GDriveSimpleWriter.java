package org.onedatashare.transferservice.odstransferservice.service.step.googleDrive;

import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.client.http.FileContent;
import com.google.api.client.http.InputStreamContent;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FileBuffer;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.AfterWrite;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.annotation.BeforeWrite;
import org.springframework.batch.item.ItemWriter;
import org.springframework.retry.support.RetryTemplate;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLConnection;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;

public class GDriveSimpleWriter implements ItemWriter<DataChunk> {
    Logger logger = LoggerFactory.getLogger(GDriveSimpleWriter.class);
    private final EntityInfo fileInfo;
    private final OAuthEndpointCredential credential;
    Drive client;
    private String basePath;
    FileBuffer fileBuffer;
    private String fileName;
    private String mimeType;
    private File fileMetaData;
    private File parentFolder;
    private RetryTemplate retryTemplate;

    public GDriveSimpleWriter(OAuthEndpointCredential credential, EntityInfo fileInfo){
        this.credential = credential;
        this.fileInfo = fileInfo;
        this.fileBuffer = new FileBuffer();
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) throws IOException, GeneralSecurityException {
        this.client = ODSUtility.authenticateDriveClient(this.credential);
        this.basePath = stepExecution.getJobExecution().getJobParameters().getString(ODSConstants.DEST_BASE_PATH);
        this.parentFolder = ODSUtility.gdriveMakeDir(this.basePath, this.client);
        logger.info("ParentFolder={} and the basePath={}", this.parentFolder, this.basePath);
    }

    @BeforeWrite
    public void beforeWrite(List<? extends DataChunk> items) {
        this.fileName = items.get(0).getFileName();
        this.mimeType = URLConnection.guessContentTypeFromName(this.fileName);
    }

    @Override
    public void write(List<? extends DataChunk> items) {
        fileBuffer.addAllChunks(items);
    }

    public void setRetryTemplate(RetryTemplate retryTemplate) {
        this.retryTemplate = retryTemplate;
    }

    @AfterStep
    public void afterStep() throws Exception {
        this.retryTemplate.execute((c) -> {
            try {
                logger.debug("Transferring file to the server");
                InputStream inputStream = this.fileBuffer.condenseListToOneStream(this.fileInfo.getSize());
                InputStreamContent inputStreamContent = new InputStreamContent(this.mimeType, inputStream);
                this.fileMetaData = new File()
                        .setName(this.fileName)
                        .setParents(Collections.singletonList(this.parentFolder.getId()))
                        .setMimeType(this.mimeType);
                File file = this.client.files().create(this.fileMetaData, inputStreamContent).execute();
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                this.client.files().get(file.getId()).executeMediaAndDownloadTo(outputStream);
                inputStream.close();
                if (outputStream.size() != fileInfo.getSize()) {
                    outputStream.close();
                    throw new IOException("Source and Destination Files do not match. Retrying file transfer...");
                }
            } catch(IOException e){
                throw e;
            }
            this.client = null;
            this.fileBuffer.clear();
            this.fileBuffer = null;
            return null;
        });
    }
}
