package org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.*;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.FileHashValidator;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.service.SetFileHash;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.onedatashare.transferservice.odstransferservice.utility.S3Utility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;


import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.SIXTYFOUR_KB;

public class AmazonS3Reader extends AbstractItemCountingItemStreamItemReader<DataChunk> implements SetFileHash {

    private final EntityInfo fileInfo;
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
    FileHashValidator fileHashValidator;
    MessageDigest messageDigest;

    public AmazonS3Reader(AccountEndpointCredential sourceCredential, EntityInfo fileInfo) {
        this.sourceCredential = sourceCredential;
        this.regionAndBucket = this.sourceCredential.getUri().split(":::");
        this.chunkSize = fileInfo.getChunkSize();
        this.partitioner = new FilePartitioner(this.chunkSize);
        this.s3Client = S3Utility.constructClient(this.sourceCredential, regionAndBucket[0]);
        this.fileInfo = fileInfo;
        this.setName(ClassUtils.getShortName(AmazonS3Reader.class));
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) throws NoSuchAlgorithmException {
        this.fileName = this.fileInfo.getId();//For an S3 Reader job this should be the object key
        this.sourcePath = stepExecution.getJobExecution().getJobParameters().getString(ODSConstants.SOURCE_BASE_PATH);
        this.amazonS3URI = new AmazonS3URI(S3Utility.constructS3URI(this.sourceCredential.getUri(), this.fileName, this.sourcePath));
        this.getSkeleton = new GetObjectRequest(this.amazonS3URI.getBucket(), this.amazonS3URI.getKey());
        logger.info("Starting the job for this file: " + this.fileName);
        messageDigest = MessageDigest.getInstance(fileHashValidator.getAlgorithm());
        fileHashValidator.setReaderMessageDigest(messageDigest);
    }

    public void setName(String name) {
        this.setExecutionContextName(name);
    }


    @Override
    protected DataChunk doRead() throws Exception {
        FilePart part = partitioner.nextPart();
        if (part == null || part.getStart() == part.getEnd()) return null;
        logger.info("Current Part:-"+part.toString());
        S3Object partOfFile = this.s3Client.getObject(this.getSkeleton.withRange(part.getStart(), part.getEnd()));//this is inclusive or on both start and end so take one off so there is no colision
        byte[] dataSet = new byte[part.getSize()];
        long totalBytes = 0;
        S3ObjectInputStream stream = partOfFile.getObjectContent();
        while (totalBytes < part.getSize()) {
            int bytesRead = 0;
            bytesRead += stream.read(dataSet, Long.valueOf(totalBytes).intValue(), Long.valueOf(part.getSize()).intValue());
            if (bytesRead == -1) return null;
            if(fileHashValidator.isVerify()) fileHashValidator.getReaderMessageDigest().update(dataSet, 0, bytesRead);
            totalBytes += bytesRead;
        }
        stream.close();
        return ODSUtility.makeChunk(part.getSize(), dataSet, part.getStart(), Long.valueOf(part.getPartIdx()).intValue(), this.fileName);
    }

    @Override
    protected void doOpen() {
        logger.info(this.amazonS3URI.toString());
        this.currentFileMetaData = this.s3Client.getObjectMetadata(this.amazonS3URI.getBucket(), this.amazonS3URI.getKey());
        partitioner.createParts(this.currentFileMetaData.getContentLength(), this.fileName);
    }

    @Override
    protected void doClose() throws Exception {
        this.s3Client = null;
    }

    @AfterStep
    public void afterStep()  {
//        String encodedHash = Base64.getEncoder().encodeToString(messageDigest.digest());
//        fileHashValidator.setReaderHash(encodedHash);
//        logger.info("Reader hash " + encodedHash);
    }

    @Override
    public void setFileHashValidator(FileHashValidator fileHash) {
        fileHashValidator = fileHash;
    }
}
