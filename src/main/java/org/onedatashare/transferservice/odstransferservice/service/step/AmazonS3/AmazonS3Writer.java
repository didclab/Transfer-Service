package org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.codec.binary.Hex;
import org.onedatashare.transferservice.odstransferservice.model.AWSMultiPartMetaData;
import org.onedatashare.transferservice.odstransferservice.model.AWSSinglePutRequestMetaData;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.FileHashValidator;
import org.onedatashare.transferservice.odstransferservice.service.SetFileHash;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.onedatashare.transferservice.odstransferservice.utility.S3Utility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.FIVE_MB;


public class AmazonS3Writer implements ItemWriter<DataChunk>, SetFileHash {

    Logger logger = LoggerFactory.getLogger(AmazonS3Writer.class);
    private final AccountEndpointCredential destCredential;
    AmazonS3URI s3URI;
    private AWSMultiPartMetaData metaData;
    private AWSSinglePutRequestMetaData singlePutRequestMetaData;
    boolean multipartUpload;
    String fileName;
    EntityInfo fileInfo;
    long currentFileSize;
    private AmazonS3 client;
    private String destBasepath;
    private boolean firstPass;
    private FileHashValidator fileHashValidator;

    public AmazonS3Writer(AccountEndpointCredential destCredential, EntityInfo fileInfo) {
        this.fileName = "";
        this.fileInfo = fileInfo;
        this.destCredential = destCredential;
        this.firstPass = false;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) throws NoSuchAlgorithmException {
        logger.info("Before Step of AmazonS3Writer and the step name is {}", stepExecution.getStepName());
        this.currentFileSize = this.fileInfo.getSize();
        logger.info("The S3 EntityInfo file is as follows: " + this.fileInfo.toString());
        this.destBasepath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        this.fileName = stepExecution.getStepName();
        this.s3URI = new AmazonS3URI(S3Utility.constructS3URI(this.destCredential.getUri(), this.fileName, destBasepath));//for aws the step name will be the file key.
        logger.info(this.s3URI.toString());
        MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
        fileHashValidator.setWriterMessageDigest(messageDigest);
    }

    public void prepareS3Transfer(String fileName) {
        if(!this.firstPass){
            this.client = createClientWithCreds();
            this.s3URI = new AmazonS3URI(S3Utility.constructS3URI(this.destCredential.getUri(), fileName, destBasepath));//for aws the step name will be the file key.
            if (this.currentFileSize < FIVE_MB) {
                this.multipartUpload = false;
                this.singlePutRequestMetaData = new AWSSinglePutRequestMetaData();
            } else {
                this.multipartUpload = true;
                this.metaData = new AWSMultiPartMetaData();
                this.metaData.prepareMetaData(client, this.s3URI.getBucket(), this.s3URI.getKey());
            }
            this.firstPass = true;
        }
    }

    @Override
    public void write(List<? extends DataChunk> items) {
        prepareS3Transfer(items.get(0).getFileName());
        StringBuilder eTagMultipart = new StringBuilder();
        if (!this.multipartUpload) {
            this.singlePutRequestMetaData.addAllChunks(items);
            DataChunk lastChunk = items.get(items.size() - 1);
            if (lastChunk.getStartPosition() + lastChunk.getSize() == this.currentFileSize) {
                PutObjectRequest putObjectRequest = new PutObjectRequest(this.s3URI.getBucket(), this.s3URI.getKey(), this.singlePutRequestMetaData.condenseListToOneStream(this.currentFileSize), makeMetaDataForSinglePutRequest(this.currentFileSize));
                PutObjectResult response = client.putObject(putObjectRequest);
                eTagMultipart.append(response.getETag());
                this.fileHashValidator.setWriterHash(response.getETag());
            }
        } else {
            //Does multipart upload to s3 bucket
            for (DataChunk currentChunk : items) {
                logger.info(currentChunk.toString());
                if (currentChunk.getStartPosition() + currentChunk.getSize() == this.currentFileSize) {
                    logger.info("At the last chunk of the transfer {}", currentChunk.getChunkIdx());
                    UploadPartRequest lastPart = ODSUtility.makePartRequest(currentChunk, this.s3URI.getBucket(), this.metaData.getInitiateMultipartUploadResult().getUploadId(), this.s3URI.getKey(), true);
                    UploadPartResult uploadPartResult = client.uploadPart(lastPart);
                    eTagMultipart.append(uploadPartResult.getETag());
                    this.metaData.addUploadPart(uploadPartResult);
                } else {
                    UploadPartRequest uploadPartRequest = ODSUtility.makePartRequest(currentChunk, this.s3URI.getBucket(), this.metaData.getInitiateMultipartUploadResult().getUploadId(), this.s3URI.getKey(), false);
                    UploadPartResult uploadPartResult = client.uploadPart(uploadPartRequest);
                    eTagMultipart.append(uploadPartResult.getETag());
                    this.metaData.addUploadPart(uploadPartResult);
                }
            }
            fileHashValidator.setWriterHash(eTagMultipart.toString());
        }
    }

    /**
     * For a non- multipart transfer, s3 returns the checksum in eTag, using md5 algorithm as a hex string.
     * For a multipart transfer, checksum is returned for each chunk/part. We concatenate these and compare with the
     *  reader hash which is generated in a similar manner
     */
    @AfterStep
    public void afterStep() {
        boolean match;
        if(multipartUpload){
            match = fileHashValidator.getReaderHash().equals(fileHashValidator.getWriterHash());
        }else{
            byte[] digest = fileHashValidator.getReaderMessageDigest().digest();
            String readerHashInHex = Hex.encodeHexString(digest);
            match = readerHashInHex.equals(fileHashValidator.getWriterHash());
        }
        logger.info("Match: "+ match);

        if (this.multipartUpload) {
            this.metaData.completeMultipartUpload(client);
            this.metaData.reset();
        }else{
            this.singlePutRequestMetaData.clear();
        }
    }

    public AmazonS3 createClientWithCreds() {
        if (this.client != null) return this.client;
        logger.info("Creating credentials for {}", this.destCredential.getUsername());
        AWSCredentials credentials = new BasicAWSCredentials(this.destCredential.getUsername(), this.destCredential.getSecret());
        AmazonS3 client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(this.s3URI.getRegion())
                .build();
        return client;
    }

    public ObjectMetadata makeMetaDataForSinglePutRequest(long size) {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(size);
        return objectMetadata;
    }

    @Override
    public void setFileHashValidator(FileHashValidator fileHash) {
        fileHashValidator = fileHash;
    }
}

