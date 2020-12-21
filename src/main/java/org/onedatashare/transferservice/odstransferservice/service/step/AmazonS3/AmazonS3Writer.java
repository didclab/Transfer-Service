package org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.*;
import com.google.common.io.ByteSource;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;


public class AmazonS3Writer implements ItemWriter<DataChunk> {

    AccountEndpointCredential destCredential;
    AmazonS3URI s3URI;
    String destBasepath;
    AmazonS3 s3Client;
    HashMap<String, AmazonS3> clientHashMap;
    List<UploadPartResult> uploadResult;
    List<PartETag> eTagList;

    public AmazonS3Writer(AccountEndpointCredential destCredential){
        this.destCredential = destCredential;
        this.s3URI = new AmazonS3URI(destCredential.getUri());
        clientHashMap = new HashMap<>();
        this.uploadResult = new ArrayList<>();
        this.eTagList = new ArrayList<>();
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution){
        this.destBasepath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        if(!this.clientHashMap.containsKey(stepExecution.getStepName())) this.clientHashMap.put(stepExecution.getStepName(), createConnection());
    }

    @AfterStep
    public void afterStep(){
        this.eTagList.clear();
        this.uploadResult.clear();
    }

    public AmazonS3 createConnection(){
        AWSCredentials credentials = new BasicAWSCredentials(this.destCredential.getUsername(), this.destCredential.getSecret());
        Regions clientRegion = Regions.fromName(new String(this.destCredential.getEncryptedSecret()));
        return AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(clientRegion)
                .build();
    }

    @Override
    public void write(List<? extends DataChunk> items) throws Exception {
        InitiateMultipartUploadRequest initiateMultipartUploadRequest = new InitiateMultipartUploadRequest(this.s3URI.getBucket(), this.s3URI.getKey());
        InitiateMultipartUploadResult initiateMultipartUploadResult = this.s3Client.initiateMultipartUpload(initiateMultipartUploadRequest);
        for(DataChunk chunk : items){
            if(this.clientHashMap.containsKey(chunk.getFileName())){
                UploadPartRequest requestChunk = new UploadPartRequest()
                        .withBucketName(this.s3URI.getBucket())
                        .withFileOffset(chunk.getStartPosition())
                        .withPartSize(chunk.getSize())
                        .withInputStream(ByteSource.wrap(chunk.getData()).openStream())
                        .withKey(this.s3URI.getKey())
                        .withUploadId(initiateMultipartUploadResult.getUploadId())
                        .withPartNumber((int) chunk.getChunkIdx());
                UploadPartResult uploadPartResult = this.s3Client.uploadPart(requestChunk);
                this.eTagList.add(uploadPartResult.getPartETag());
                this.uploadResult.add(uploadPartResult);
            }
        }
        CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest();
        completeMultipartUploadRequest.setUploadId(initiateMultipartUploadResult.getUploadId());
        completeMultipartUploadRequest.withBucketName(this.s3URI.getBucket());
        completeMultipartUploadRequest.setKey(this.s3URI.getKey());
        completeMultipartUploadRequest.setPartETags(this.eTagList);
        CompleteMultipartUploadResult completeMultipartUploadResult = this.s3Client.completeMultipartUpload(completeMultipartUploadRequest);
    }
}
