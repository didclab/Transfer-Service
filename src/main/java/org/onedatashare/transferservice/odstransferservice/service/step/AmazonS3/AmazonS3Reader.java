package org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.*;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.onedatashare.transferservice.odstransferservice.utility.S3Utility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.util.ClassUtils;


import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.SIXTYFOUR_KB;

public class AmazonS3Reader extends AbstractItemCountingItemStreamItemReader<DataChunk> implements ResourceAwareItemReaderItemStream<DataChunk>, InitializingBean {

    Logger logger = LoggerFactory.getLogger(AmazonS3Reader.class);
    private AmazonS3 s3Client;
    private AmazonS3URI amazonS3URI;
    private final FilePartitioner partitioner;
    private String sourcePath;
    String fileName;
    String[] regionAndBucket;
    private final AccountEndpointCredential sourceCredential;
    private final int chunkSize;
    ObjectMetadata currentFileMetaData;
    GetObjectRequest getSkeleton;

    public AmazonS3Reader(AccountEndpointCredential sourceCredential, int chunkSize) {
        this.sourceCredential = sourceCredential;
        this.regionAndBucket = this.sourceCredential.getUri().split(":::");
        this.chunkSize = Math.max(SIXTYFOUR_KB, chunkSize);
        this.partitioner = new FilePartitioner(this.chunkSize);
        this.setName(ClassUtils.getShortName(AmazonS3Reader.class));
        this.s3Client = S3Utility.constructClient(this.sourceCredential, regionAndBucket[0]);
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.fileName = stepExecution.getStepName();//For an S3 Reader job this should be the object key
        this.sourcePath = stepExecution.getJobExecution().getJobParameters().getString(ODSConstants.SOURCE_BASE_PATH);
        this.amazonS3URI = new AmazonS3URI(S3Utility.constructS3URI(this.sourceCredential, this.fileName, this.sourcePath));
        this.getSkeleton = new GetObjectRequest(this.amazonS3URI.getBucket(), this.amazonS3URI.getKey());
        logger.info("Starting the job for this file: " + this.fileName);
    }

    @AfterStep
    public void afterStep(StepExecution stepExecution) {
    }

    public void setName(String name) {
        this.setExecutionContextName(name);
    }

    @Override
    public void setResource(Resource resource) {
    }


    @Override
    protected DataChunk doRead() throws Exception {
        FilePart part = partitioner.nextPart();
        if (part == null) return null;
        logger.info("Current Part:-"+part.toString());
        S3Object partOfFile = this.s3Client.getObject(this.getSkeleton.withRange(part.getStart(), part.getEnd()));//this is inclusive or on both start and end so take one off so there is no colision
        byte[] dataSet = null;
        if(!part.isLastChunk()){
            dataSet = new byte[this.chunkSize];
        }
        else
            dataSet = new byte[part.getSize()];
        long totalBytes = 0;
        S3ObjectInputStream stream = partOfFile.getObjectContent();
        while (totalBytes < part.getSize()) {
            int bytesRead = 0;
            bytesRead += stream.read(dataSet, Long.valueOf(totalBytes).intValue(), Long.valueOf(part.getSize()).intValue());
            if (bytesRead == -1) return null;
            totalBytes += bytesRead;
        }
        stream.close();
        return ODSUtility.makeChunk(chunkSize, dataSet, part.getStart(), Long.valueOf(part.getPartIdx()).intValue(), this.fileName);
    }

    @Override
    protected void doOpen() throws Exception {
        this.currentFileMetaData = this.s3Client.getObjectMetadata(this.amazonS3URI.getBucket(), this.amazonS3URI.getKey());
        partitioner.createParts(this.currentFileMetaData.getContentLength(), this.fileName);
    }

    @Override
    protected void doClose() throws Exception {
    }

    @Override
    public void afterPropertiesSet() throws Exception {
    }
}
