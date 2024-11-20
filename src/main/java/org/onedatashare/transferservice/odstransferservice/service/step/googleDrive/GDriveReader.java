package org.onedatashare.transferservice.odstransferservice.service.step.googleDrive;

import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.onedatashare.commonutils.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class GDriveReader extends AbstractItemCountingItemStreamItemReader<DataChunk> {

    Logger logger = LoggerFactory.getLogger(GDriveReader.class);
    private final EntityInfo fileInfo;
    private final OAuthEndpointCredential credential;
    private final FilePartitioner partitioner;
    private Drive client;
    private String fileName;
    private File file;

    public GDriveReader(OAuthEndpointCredential credential, EntityInfo fileInfo) {
        this.credential = credential;
        this.fileInfo = fileInfo;
        this.partitioner = new FilePartitioner(fileInfo.getChunkSize());
        this.setName(ClassUtils.getShortName(GDriveReader.class));
        logger.info(credential.toString());
    }

    @Override
    protected DataChunk doRead() throws Exception {
        FilePart filePart = this.partitioner.nextPart();
        if (filePart == null || filePart.getSize() == 0) return null;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(filePart.getSize());
        try {
            Drive.Files.Get get = this.client.files().get(fileInfo.getId());
            get.getMediaHttpDownloader().setChunkSize(this.fileInfo.getChunkSize()).setContentRange(filePart.getStart(), filePart.getEnd());
            get.executeMediaAndDownloadTo(outputStream);
            byte[] receivedData = outputStream.toByteArray();
            if (receivedData.length != filePart.getSize()) {
                logger.warn("Invalid chunk size received. Retrying to read chunk from the server...");
                throw new IOException("Chunk receive error.");
            }
        } catch (IOException e) {
            throw e;
        }
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
    }

    @Override
    protected void doClose() {
        this.client = null;
    }
}