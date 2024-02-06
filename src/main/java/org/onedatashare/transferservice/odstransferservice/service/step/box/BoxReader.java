package org.onedatashare.transferservice.odstransferservice.service.step.box;

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

import java.io.ByteArrayOutputStream;

public class BoxReader extends AbstractItemCountingItemStreamItemReader<DataChunk> {

    private OAuthEndpointCredential credential;
    FilePartitioner filePartitioner;
    private BoxAPIConnection boxAPIConnection;
    private BoxFile currentFile;
    EntityInfo fileInfo;
    int retry;
    Logger logger = LoggerFactory.getLogger(BoxReader.class);

    public BoxReader(OAuthEndpointCredential credential, EntityInfo fileInfo) {
        this.credential = credential;
        this.setName(ClassUtils.getShortName(BoxReader.class));
        filePartitioner = new FilePartitioner(fileInfo.getChunkSize());
        this.fileInfo = fileInfo;
        retry = 1;
    }

    /**
     * Read in those chunks
     *
     * @return
     */
    @Override
    protected DataChunk doRead() {
        FilePart filePart = filePartitioner.nextPart();
        if (filePart == null) return null;
        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        if(this.fileInfo.getSize() == this.fileInfo.getChunkSize()){
            this.currentFile.download(byteArray);
        }else{
            this.currentFile.downloadRange(byteArray, filePart.getStart(), filePart.getEnd());
        }
        DataChunk chunk = ODSUtility.makeChunk(filePart.getSize(), byteArray.toByteArray(), filePart.getStart(), Math.toIntExact(filePart.getPartIdx()), currentFile.getInfo().getName());
        logger.info(chunk.toString());
        return chunk;
    }

    /**
     * Open your connections, and get your streams
     *
     * @throws Exception
     */
    @Override
    protected void doOpen() {
        filePartitioner.createParts(this.fileInfo.getSize(), this.fileInfo.getId());
        this.boxAPIConnection = new BoxAPIConnection(credential.getToken());
        this.currentFile = new BoxFile(this.boxAPIConnection, this.fileInfo.getId());
        this.currentFile.getInfo("name");
        this.boxAPIConnection.setMaxRetryAttempts(this.retry);
    }

    /**
     * Close your connection and destroy any clients used
     *
     * @throws Exception
     */
    @Override
    protected void doClose() {
        this.boxAPIConnection = null;
    }

    public void setMaxRetry(int attempt) {
        this.retry = attempt;
    }
}
