package org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.onedatashare.commonservice.model.credential.AccountEndpointCredential;
import lombok.Setter;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.pools.S3ConnectionPool;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.onedatashare.transferservice.odstransferservice.utility.S3Utility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

public class AmazonS3Reader extends AbstractItemCountingItemStreamItemReader<DataChunk> {

    private final EntityInfo fileInfo;
    Logger logger = LoggerFactory.getLogger(AmazonS3Reader.class);
    private AmazonS3 s3Client;
    private AmazonS3URI amazonS3URI;
    private final FilePartitioner partitioner;
    String fileName;
    String[] regionAndBucket;
    private final AccountEndpointCredential sourceCredential;
    ObjectMetadata currentFileMetaData;
    GetObjectRequest getSkeleton;
    @Setter
    S3ConnectionPool pool;

    public AmazonS3Reader(AccountEndpointCredential sourceCredential, EntityInfo fileInfo) {
        this.sourceCredential = sourceCredential;
        this.regionAndBucket = this.sourceCredential.getUri().split(":::");
        this.partitioner = new FilePartitioner(fileInfo.getChunkSize());
        this.fileInfo = fileInfo;
        this.setName(ClassUtils.getShortName(AmazonS3Reader.class));
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.fileName = this.fileInfo.getId();//For an S3 Reader job this should be the object key
        this.amazonS3URI = new AmazonS3URI(S3Utility.constructS3URI(this.sourceCredential.getUri(), this.fileName));
        this.getSkeleton = new GetObjectRequest(this.amazonS3URI.getBucket(), this.amazonS3URI.getKey());
        logger.info("Starting S3 job for file {} with uri {}", this.fileName, this.amazonS3URI);
    }

    public void setName(String name) {
        this.setExecutionContextName(name);
    }


    @Override
    protected DataChunk doRead() throws Exception {
        FilePart part = partitioner.nextPart();
        if (part == null || part.getStart() == part.getEnd()) return null;
        logger.info("Current Part:-" + part.toString());
        S3Object partOfFile = this.s3Client.getObject(this.getSkeleton.withRange(part.getStart(), part.getEnd()));//this is inclusive or on both start and end so take one off so there is no colision
        byte[] dataSet = new byte[part.getSize()];
        long totalBytes = 0;
        S3ObjectInputStream stream = partOfFile.getObjectContent();
        while (totalBytes < part.getSize()) {
            int bytesRead = 0;
            bytesRead += stream.read(dataSet, Long.valueOf(totalBytes).intValue(), Long.valueOf(part.getSize()).intValue());
            if (bytesRead == -1) return null;
            totalBytes += bytesRead;
        }
        stream.close();
        return ODSUtility.makeChunk(part.getSize(), dataSet, part.getStart(), Long.valueOf(part.getPartIdx()).intValue(), this.fileName);
    }

    @Override
    protected void doOpen() throws InterruptedException {
        logger.info(this.amazonS3URI.toString());
        this.s3Client = this.pool.borrowObject();
        this.currentFileMetaData = this.s3Client.getObjectMetadata(this.amazonS3URI.getBucket(), this.amazonS3URI.getKey());
        String key = this.amazonS3URI.getKey();
        int idx = key.lastIndexOf("/");
        if (idx > -1) {
            this.fileName = key.substring(idx + 1);
        }
        partitioner.createParts(this.currentFileMetaData.getContentLength(), this.fileName);
    }

    @Override
    protected void doClose() {
        this.pool.returnObject(this.s3Client);
    }

    public void setPool(S3ConnectionPool s3ReaderPool) {
        this.pool = s3ReaderPool;
    }
}
