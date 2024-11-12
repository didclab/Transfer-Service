package org.onedatashare.transferservice.odstransferservice.service.step.googleDrive;

import com.google.api.client.http.InputStreamContent;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.SmallFileUpload;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.annotation.BeforeWrite;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

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
    SmallFileUpload smallFileUpload;
    private String fileName;
    private String mimeType;
    private File fileMetaData;
    private File parentFolder;

    public GDriveSimpleWriter(OAuthEndpointCredential credential, EntityInfo fileInfo) {
        this.credential = credential;
        this.fileInfo = fileInfo;
        this.smallFileUpload = new SmallFileUpload();
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
    public void write(Chunk<? extends DataChunk> items) {
        this.smallFileUpload.addAllChunks(items.getItems());
    }


    @AfterStep
    public void afterStep() throws Exception {
        try {
            logger.debug("Transferring file to the server");
            InputStream inputStream = this.smallFileUpload.condenseListToOneStream();
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
        } catch (IOException e) {
            throw e;
        }
        this.client = null;
        this.smallFileUpload.clearBuffer();
        this.smallFileUpload = null;
    }
}
