package org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.onedatashare.transferservice.odstransferservice.model.S3DataChunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class AmazonS3Reader<T> extends AbstractItemCountingItemStreamItemReader<S3DataChunk> implements ResourceAwareItemReaderItemStream<S3DataChunk>, InitializingBean {

    Logger logger = LoggerFactory.getLogger(AmazonS3Reader.class);
    private String s3BucketName;
    private String s3ObjectName;
    private Resource resource;
    private static final long STANDARD_SIZE = 5*1024*1024;
    private AmazonS3 s3Client;
    private List<S3ObjectSummary> objectSummaries;
    private List<InputStream> streamList = new ArrayList<>();
    private InputStream inputStream;
    private String fileType;
    private long largeFileSize;
    private long fileSize;


    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        //s3BucketName = stepExecution.getJobParameters().getString(SOURCE_BASE_PATH);
        //s3ObjectName = stepExecution.getStepName();
        s3BucketName = "test-transfer-service-bucket";
        s3ObjectName = "test-key.pdf";
        String s3AccessId = "AKIASFB52FW72R74YEDX";
        String s3AccessKey = "57Wbk5T8KwjWtuoufef6oX8yjor3H1LgyFnwRO1P";
        AWSCredentials credentials = new BasicAWSCredentials(s3AccessId, s3AccessKey);
        Regions clientRegion = Regions.US_EAST_2;
        this.s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(clientRegion)
                .build();
    }

    public void setName(String name) {
        this.setExecutionContextName(name);
    }

    @Override
    public void setResource(Resource resource) {
        this.resource = resource;
    }

    @Override
    protected S3DataChunk doRead() throws Exception {
        S3DataChunk s3DataChunk = new S3DataChunk();
        s3DataChunk.setFileType(fileType);
        s3DataChunk.setSize(fileSize);
        s3DataChunk.setStreamList(streamList);
        return s3DataChunk;
    }

    @Override
    protected void doOpen() throws Exception {
        startDownloadingContent(s3BucketName,s3ObjectName);
    }

    @Override
    protected void doClose() throws Exception {
        try{
            if (inputStream != null) inputStream.close();
        }catch(Exception ex){
            logger.error("Not able to close the input Stream");
            ex.printStackTrace();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        //Do Nothing
    }

    /**
     * Start of the Helper Functions to get S3 Data
     */
    /**
     * Calculate the Size of the given File in the S3 Bucket
     * @param keyName
     * @param bucketName
     * @return Size of the File
     */
    private long keySize(String keyName,String bucketName) {
        long size = s3Client.getObjectMetadata(bucketName,keyName).getContentLength();
        System.out.println("Content Size: "+size);
        return size;
    }

    /**
     * Check whether the File is larger than 5 MB or Not
     * @param size
     * @return
     */
    private boolean isLongFile(long size) {
        if(size>STANDARD_SIZE) {
            return true;
        }
        return false;
    }

    /**
     * List All the Objects (Files in case of S3) from a given S3 Bucket
     * @param bucketName
     * @return
     */
    public List<S3ObjectSummary> listObjectFromS3Bucket(String bucketName) {
        System.out.format("Objects in S3 bucket %s:\n", bucketName);
        ListObjectsV2Result result = s3Client.listObjectsV2(bucketName);
        objectSummaries = result.getObjectSummaries();
        return objectSummaries;
    }
    /**
     * Downloading File Content For a Small File
     * @param keyName
     * @param bucketName
     */
    public void downloadS3FileContentForSmallFiles(String keyName,String bucketName){
        S3Object fullObject = s3Client.getObject(new GetObjectRequest(bucketName, keyName));
        inputStream = fullObject.getObjectContent();
        fileType = fullObject.getObjectMetadata().getContentType();
        fileSize = fullObject.getObjectMetadata().getContentLength();
        streamList.add(inputStream);
        System.out.println("Total Downloaded Content Size from Normal Download: "+
                fullObject.getObjectMetadata().getContentLength());
    }

    /**
     * Partial Download For S3
     * Performs Seek Operation
     * Only For Large Files (Greater than 5MB)
     * @param keyName
     * @param bucketName
     */
    public void downloadS3FileContentForLargeFiles (String keyName, String bucketName) {
        long bytesDownload = 0;
        GetObjectRequest rangeObjectRequest;
        try {
            System.out.println("Downloading an object");
            while(bytesDownload<largeFileSize){
                InputStream localInputStream;
                System.out.println("Downloaded: "+bytesDownload);
                if((largeFileSize-bytesDownload)>STANDARD_SIZE) {
                    rangeObjectRequest = new GetObjectRequest(bucketName, keyName)
                            .withRange(bytesDownload, STANDARD_SIZE);
                    bytesDownload += STANDARD_SIZE;
                }
                else {
                    System.out.println("Bytes: "+bytesDownload);
                    rangeObjectRequest = new GetObjectRequest(bucketName, keyName)
                            .withRange(bytesDownload, largeFileSize);
                    bytesDownload += largeFileSize-bytesDownload;
                }
                bytesDownload++;
                S3Object objectPortion = s3Client.getObject(rangeObjectRequest);
                localInputStream = objectPortion.getObjectContent();
                streamList.add(localInputStream);
            }
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        } catch (SdkClientException e) {
            e.printStackTrace();
        }
    }

    /**
     * Trigger Downloading Content From a S3 Object
     * @throws IOException
     */
    public void startDownloadingContent(String s3BucketName,String s3ObjectName) throws IOException {
        listObjectFromS3Bucket(s3BucketName);
        String objectSummary = null;
        String fileName = null;
        for (S3ObjectSummary s3ObjectSummary : objectSummaries){
            objectSummary = s3ObjectSummary.getKey();
            if(objectSummary.equals(s3ObjectName)){
                fileName = objectSummary;
            }
        }
        largeFileSize = keySize(fileName,s3BucketName);
        if(isLongFile(largeFileSize)){
            downloadS3FileContentForLargeFiles(fileName,s3BucketName);
        }
        else {
            downloadS3FileContentForSmallFiles(fileName, s3BucketName);
        }
    }

}
