package org.onedatashare.transferservice.odstransferservice.service.step.box;

import java.io.ByteArrayOutputStream;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxFile;

import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

public class BoxReader extends AbstractItemCountingItemStreamItemReader<DataChunk> {

    private OAuthEndpointCredential credential;
    FilePartitioner filePartitioner;
    private BoxAPIConnection boxAPIConnection;
    private BoxFile currentFile;
    EntityInfo fileInfo;
    Logger logger = LoggerFactory.getLogger(BoxReader.class);

    public BoxReader(OAuthEndpointCredential credential, EntityInfo fileInfo){
        this.credential = credential;
        this.setName(ClassUtils.getShortName(BoxReader.class));
        filePartitioner = new FilePartitioner(fileInfo.getChunkSize());
        this.fileInfo = fileInfo;
    }

    /**
     * This gets called before every single step executes and every step represents a single file fyi
     * @param stepExecution
     */
    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        filePartitioner.createParts(this.fileInfo.getSize(), this.fileInfo.getId());
    }

    /**
     * Read in those chunks
     * @return
     */
    @Override
    protected DataChunk doRead() {
        FilePart filePart = filePartitioner.nextPart();
        if (filePart == null) return null;
        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        this.currentFile.downloadRange(byteArray, filePart.getStart(), filePart.getEnd());
        DataChunk chunk = ODSUtility.makeChunk(filePart.getSize(), byteArray.toByteArray(), filePart.getStart(), Math.toIntExact(filePart.getPartIdx()), currentFile.getInfo().getName());
        logger.info(chunk.toString());
        return chunk;
    }

    /**
     * Open your connections, and get your streams
     * @throws Exception
     */
    @Override
    protected void doOpen() {
        this.boxAPIConnection = new BoxAPIConnection(credential.getToken());
        this.currentFile = new BoxFile(this.boxAPIConnection, this.fileInfo.getId());
    }

    /**
     * Close your connection and destroy any clients used
     * @throws Exception
     */
    @Override
    protected void doClose() {
        this.boxAPIConnection = null;
    }

    public void setMaxRetry(int attempt) {
        this.boxAPIConnection.setMaxRetryAttempts(attempt);
    }
}
