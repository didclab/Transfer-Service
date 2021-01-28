package org.onedatashare.transferservice.odstransferservice.model;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@NoArgsConstructor
@Getter
@Setter
public class AWSMultiPartMetaData {
    private InitiateMultipartUploadResult initiateMultipartUploadResult;
    private InitiateMultipartUploadRequest initiateMultipartUploadRequest;
    private List<UploadPartResult> uploadResult;
    boolean prepared;

    public void prepareMetaData(AmazonS3 client,String bucketName, String fileKey) {
        this.initiateMultipartUploadRequest = new InitiateMultipartUploadRequest(bucketName, fileKey);
        this.initiateMultipartUploadResult = client.initiateMultipartUpload(this.initiateMultipartUploadRequest);
        this.prepared = true;
        uploadResult = new ArrayList<>();
    }

    public CompleteMultipartUploadResult completeMultipartUpload(AmazonS3 client){
        CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest();
        completeMultipartUploadRequest.setBucketName(this.initiateMultipartUploadRequest.getBucketName());
        completeMultipartUploadRequest.setKey(this.initiateMultipartUploadRequest.getKey());
        completeMultipartUploadRequest.withPartETags(this.getUploadResult());
        completeMultipartUploadRequest.setUploadId(this.initiateMultipartUploadResult.getUploadId());
        return client.completeMultipartUpload(completeMultipartUploadRequest);
    }

    public void addUploadPart(UploadPartResult result){
        this.uploadResult.add(result);
    }

    public void reset(){
        this.initiateMultipartUploadRequest = null;
        this.initiateMultipartUploadResult = null;
        this.uploadResult.clear();
        this.prepared = false;
    }
}
