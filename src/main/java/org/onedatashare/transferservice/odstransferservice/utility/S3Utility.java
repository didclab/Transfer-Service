package org.onedatashare.transferservice.odstransferservice.utility;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;

public class S3Utility {

    public static AmazonS3 constructClient(AccountEndpointCredential credential, String region){
        AWSCredentials credentials = new BasicAWSCredentials(credential.getUsername(), credential.getSecret());
        return AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(region)
                .build();
    }

    public static String constructS3URI(String uri, String fileKey){
        StringBuilder builder = new StringBuilder();
        String[] temp = uri.split(":::");
        String bucketName = temp[1];
        String region = temp[0];
        builder.append("https://").append(bucketName).append(".").append("s3.").append(region).append(".").append("amazonaws.com/").append(fileKey);
        return builder.toString();
    }
}
