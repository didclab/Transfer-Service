package org.onedatashare.transferservice.odstransferservice.service.step.box;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxFile;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3.AmazonS3Reader;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.util.ClassUtils;

import java.io.ByteArrayOutputStream;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.SIXTYFOUR_KB;

public class BoxReader extends AbstractItemCountingItemStreamItemReader<DataChunk> implements ResourceAwareItemReaderItemStream<DataChunk>, InitializingBean {

    OAuthEndpointCredential credential;
    int chunkSize;
    FilePartitioner filePartitioner;
    private BoxAPIConnection boxAPIConnection;
    private String sourcePath;
    private BoxFile currentFile;
    Logger logger = LoggerFactory.getLogger(BoxReader.class);
    EntityInfo fileInfo;
    private BoxFile.Info boxFileInfo;

    public BoxReader(OAuthEndpointCredential credential, int chunkSize, EntityInfo fileInfo){
        this.credential = credential;
        this.setName(ClassUtils.getShortName(BoxReader.class));
        this.chunkSize = Math.max(SIXTYFOUR_KB, chunkSize);
        filePartitioner = new FilePartitioner(this.chunkSize);
        this.boxAPIConnection = new BoxAPIConnection(credential.getToken());
        this.fileInfo = fileInfo;
    }

    /**
     * This gets called before every single step executes and every step represents a single file fyi
     * @param stepExecution
     */
    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.sourcePath = stepExecution.getJobExecution().getJobParameters().getString(ODSConstants.SOURCE_BASE_PATH);
        filePartitioner.createParts(boxFileInfo.getSize(), boxFileInfo.getName());
    }


    @Override
    public void setResource(Resource resource) {}

    /**
     * Read in those chunks
     * @return
     */
    @Override
    protected DataChunk doRead() {
        FilePart filePart = filePartitioner.nextPart();
        logger.info("Hello I am in DoRead");
        if (filePart == null) return null;
        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        this.currentFile.downloadRange(byteArray, filePart.getStart(), filePart.getEnd());
        byte[] data = byteArray.toByteArray();
        return ODSUtility.makeChunk(filePart.getSize(), data, filePart.getStart(), Math.toIntExact(filePart.getPartIdx()), this.fileInfo.getId());
    }

    /**
     * Open your connections, and get your streams
     * @throws Exception
     */
    @Override
    protected void doOpen() throws Exception {
        this.currentFile = new BoxFile(this.boxAPIConnection, this.fileInfo.getId());
        logger.info(this.currentFile.getID());
        this.boxFileInfo = this.currentFile.getInfo();
    }

    /**
     * Close your connection and destroy any clients used
     * @throws Exception
     */
    @Override
    protected void doClose() throws Exception {
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        //Only put things in here if the reader actually needs certain properties to "Read"
    }
}
