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
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.List;

@Service
public class S3Reader {

    private static final int STANDARD_SIZE = 5*1024*1024;
    private AmazonS3 s3Client;
    private List<S3ObjectSummary> objectSummaries;
    private List<PartETag> partETags = new ArrayList<>();
    private List<InputStream> streamList = new ArrayList<>();
    private InitiateMultipartUploadRequest initRequest;
    private InitiateMultipartUploadResult  initResponse;
    private S3Object fullObject = null, objectPortion =  null;
    private InputStream inputStream;
    private String fileType;
    byte[] s3Content;
    private long largeFileSize;
    private int partNumber=1;

    public S3Reader() {
        AWSCredentials credentials = new BasicAWSCredentials("AKIASFB52FW72R74YEDX",
                "57Wbk5T8KwjWtuoufef6oX8yjor3H1LgyFnwRO1P");
        Regions clientRegion = Regions.US_EAST_2;
        this.s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(clientRegion)
                .build();
        this.initRequest = new InitiateMultipartUploadRequest("test-transfer-service-bucket",
                "another-test.pdf");
        this.initResponse = s3Client.initiateMultipartUpload(initRequest);
    }

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
        fullObject = s3Client.getObject(new GetObjectRequest(bucketName, keyName));
        inputStream = fullObject.getObjectContent();
        fileType = fullObject.getObjectMetadata().getContentType();
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
    public void downloadS3FileContentForLargeFiles (String keyName, String bucketName) throws IOException {
        long bytesDownload = 0;
        GetObjectRequest rangeObjectRequest;
        try {
            System.out.println("Downloading an object");
            while(bytesDownload<largeFileSize){
                InputStream localInputStream;
                if((largeFileSize-bytesDownload)>STANDARD_SIZE) {
                    rangeObjectRequest = new GetObjectRequest(bucketName, keyName)
                            .withRange(bytesDownload, STANDARD_SIZE-1);
                    bytesDownload += STANDARD_SIZE-1;
                }
                else {
                    rangeObjectRequest = new GetObjectRequest(bucketName, keyName)
                            .withRange(bytesDownload, largeFileSize);
                    bytesDownload += largeFileSize-bytesDownload;
                }
                bytesDownload++;
                objectPortion = s3Client.getObject(rangeObjectRequest);
                System.out.println("Final Download "+(bytesDownload-1));
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
    public void startDownloadingContent() throws IOException {
        listObjectFromS3Bucket("test-transfer-service-bucket");
        String objectSummary = null;
        String fileName = null;
        for (S3ObjectSummary s3ObjectSummary : objectSummaries){
            objectSummary = s3ObjectSummary.getKey();
            if(objectSummary.equals("Naruto Shippuden - s01e01.mkv")){
                fileName = objectSummary;
            }
        }
        largeFileSize = keySize(fileName,"test-transfer-service-bucket");
        if(isLongFile(largeFileSize)){
            downloadS3FileContentForLargeFiles(fileName,"test-transfer-service-bucket");
        }
        else {
            downloadS3FileContentForSmallFiles(fileName, "test-transfer-service-bucket");
        }
    }

    /**
     * Performs Partial Upload For the S3 Content
     * Creates multiple UploadPartRequests
     * @param keyName
     * @param bucketName
     * @param byteArrayInputStream
     * @param isFinalPart
     * @param partNumber
     * @throws IOException
     */
    public void uploadPartialObjectToS3(String keyName, String bucketName, InputStream byteArrayInputStream,
                                        boolean isFinalPart, int partNumber) throws IOException {
        UploadPartRequest uploadPartRequest = new UploadPartRequest()
                .withBucketName(bucketName)
                .withKey(keyName)
                .withUploadId(initResponse.getUploadId())
                .withPartNumber(partNumber)
                .withInputStream(byteArrayInputStream)
                .withPartSize(byteArrayInputStream.available());
        if (isFinalPart) {
            uploadPartRequest.withLastPart(true);
        }
        UploadPartResult uploadResult = s3Client.uploadPart(uploadPartRequest);
        partETags.add(uploadResult.getPartETag());
        System.out.println("Response: "+uploadPartRequest.getUploadId());
    }

    /**
     * Completes the Multiple Upload Request created for different Chunks
     * @param partETags
     * @param bucketName
     * @param keyName
     */
    public void completeMultipleRequestUpload(List<PartETag> partETags,String bucketName,String keyName) {
        CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(bucketName, keyName,
                initResponse.getUploadId(), partETags);
        s3Client.completeMultipartUpload(compRequest);
    }

    /**
     * Trigger the Upload Operation for a S3 Content
     * @throws IOException
     */
    public void startUploadingContentToS3() throws IOException {
        System.out.println("Total Number of Streams: "+streamList.size());
        if(isLongFile(largeFileSize)){
            for(int i=0;i<streamList.size()-1;i++){
                partUploadingContent(streamList.get(i));
            }
            uploadLastChunk(streamList.get(streamList.size()-1));
        }
        else {
            // Do Nothing
            //Implement Normal Upload
        }
        System.out.println("Started Uploading");
        completeMultipleRequestUpload(partETags,"test-transfer-service-bucket",
                "another-test.pdf");
        System.out.println("Done Uploading");
    }

    /**
     * Takes care of the Parallelism
     * Creates chunk of 5MB to create the Multiple S3 Upload Request
     * //@param inputStream
     * @throws IOException
     */

    public void partUploadingContent(InputStream inputStream) throws IOException {
        int bytesRead =0, bytesAdded=0,leftBytes=0;
        s3Content = new byte[STANDARD_SIZE];
        s3Content = ByteStreams.toByteArray(inputStream);
        System.out.println("Size of the Stream: "+s3Content.length);
        int partSize = 5*1024*1024;
            while (bytesRead < s3Content.length) {
                byte[] add = new byte[partSize];
                InputStream byteStream;
                System.out.println("Starting Index: " + bytesAdded);
                System.out.println("Bytes Left: " + (s3Content.length - bytesRead));
                if ((s3Content.length - bytesRead) > partSize) {
                    for (int i = 0; i < partSize; i++) {
                        add[i] = s3Content[bytesAdded];
                        bytesAdded++;
                    }
                    byteStream = ByteSource.wrap(add).openStream();
                    bytesRead += partSize;
                    System.out.println("Ending Index: " + bytesAdded);
                    System.out.println("Sending Bytes for Upload: " + bytesRead);
                }
                else {
                    leftBytes = s3Content.length - bytesRead;
                    System.out.println("Byes left to Send: " + leftBytes);
                    for (int i = 0; i < leftBytes; i++) {
                        add[i] = s3Content[bytesAdded];
                        bytesAdded++;
                    }
                    byteStream = ByteSource.wrap(add).openStream();
                    bytesRead += partSize;
                }
                uploadPartialObjectToS3("another-test.pdf", "test-transfer-service-bucket",
                        byteStream, false, partNumber++);
                byteStream.reset();
            }
    }

    public void uploadLastChunk(InputStream inputStream) throws IOException {
        InputStream byteStream;
        s3Content = ByteStreams.toByteArray(inputStream);
        byteStream = ByteSource.wrap(s3Content).openStream();
        uploadPartialObjectToS3("another-test.pdf", "test-transfer-service-bucket",
                byteStream, true, partNumber++);
    }
}

