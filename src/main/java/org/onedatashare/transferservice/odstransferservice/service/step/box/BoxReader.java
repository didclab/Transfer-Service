package org.onedatashare.transferservice.odstransferservice.service.step.box;

import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3.AmazonS3Reader;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.util.ClassUtils;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.SIXTYFOUR_KB;

public class BoxReader extends AbstractItemCountingItemStreamItemReader<DataChunk> implements ResourceAwareItemReaderItemStream<DataChunk>, InitializingBean {

    OAuthEndpointCredential credential;
    int chunkSize;
    FilePartitioner filePartitioner;

    public BoxReader(OAuthEndpointCredential credential, int chunkSize){
        this.credential = credential;
        this.setName(ClassUtils.getShortName(AmazonS3Reader.class));
        this.chunkSize = Math.max(SIXTYFOUR_KB, chunkSize);
        filePartitioner = new FilePartitioner(this.chunkSize);

    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {

    }


    @Override
    public void setResource(Resource resource) {
        //leave empty as we dont use Spring "Resource"
    }

    /**
     * Read in those chunks
     * @return
     * @throws Exception
     */
    @Override
    protected DataChunk doRead() throws Exception {
        FilePart filePart = filePartitioner.nextPart();
        if (filePart == null) return null;
        return null;
    }

    /**
     * Open your connections, and get your streams
     * @throws Exception
     */
    @Override
    protected void doOpen() throws Exception {

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
