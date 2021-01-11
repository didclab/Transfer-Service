package org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.*;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
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

public class AmazonS3Reader<T> extends AbstractItemCountingItemStreamItemReader<DataChunk> implements ResourceAwareItemReaderItemStream<DataChunk>, InitializingBean {

    Logger logger = LoggerFactory.getLogger(AmazonS3Reader.class);
    private static final int STANDARD_SIZE = 5*1024*1024;
    private final AmazonS3 s3Client;
    private AmazonS3URI amazonS3URI;
    private FilePartitioner partitioner;
    String fileName;
    AccountEndpointCredential sourceCredential;
    private final int chunkSize;
    private final EntityInfo fileInfo;
    ObjectMetadata currentFileMetaData;
    GetObjectRequest getSkeleton;
    int bytesReadIn;

    public AmazonS3Reader(AccountEndpointCredential sourceCredential, int chunkSize, EntityInfo fileInfo){
        this.sourceCredential = sourceCredential;
        this.chunkSize = Math.max(SIXTYFOUR_KB, chunkSize);
        this.partitioner = new FilePartitioner(this.chunkSize);
        this.fileInfo = fileInfo;
        this.amazonS3URI = new AmazonS3URI(constructURI());//For reading from an S3 bucket the path provided in the infoList will be the Object URL
        this.getSkeleton = new GetObjectRequest(this.amazonS3URI.getBucket(), this.amazonS3URI.getKey());
        this.s3Client = constructClient();
        this.setName(ClassUtils.getShortName(AmazonS3Reader.class));
        this.bytesReadIn = 0;
    }

    public String constructURI(){
        StringBuilder builder = new StringBuilder();
        String[] temp = this.sourceCredential.getUri().split(":::");
        String bucketName = temp[1];
        String region = temp[0];
        builder.append("https://").append(bucketName).append(".").append("s3.").append(region).append(".").append("amazonaws.com").append("/").append(this.fileInfo.getPath());
        return builder.toString();
        //https://jacobstestbucket.s3.us-east-2.amazonaws.com/apache-maven-3.6.3-bin.tar
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.fileName = stepExecution.getStepName();//For an S3 Reader job this should be the object key
        logger.info("Starting the job for this file: " + this.fileName);
        this.bytesReadIn = 0;
    }

    @AfterStep
    public void afterStep(){
        logger.info("Completed the S3 step with step name " + this.fileName);
        logger.info("The bytes read for this step are " + this.bytesReadIn);
    }

    public void setName(String name) {
        this.setExecutionContextName(name);
    }

    @Override
    public void setResource(Resource resource) {}


    @Override
    protected DataChunk doRead() throws Exception {
        FilePart part = partitioner.nextPart();
        if(part == null) return null;
        logger.info(part.toString());
        S3Object partOfFile = this.s3Client.getObject(this.getSkeleton.withRange(part.getStart(), part.getEnd()-1));//this is inclusive or on both start and end so take one off so there is no colision
        logger.info(partOfFile.toString());
        byte[] dataSet = new byte[(int)part.getSize()];
        long totalBytes = 0;
        S3ObjectInputStream stream = partOfFile.getObjectContent();
        while(totalBytes < part.getSize()){
            int bytesRead = 0;
            bytesRead += stream.read(dataSet, (int) totalBytes, (int) part.getSize());
            if(bytesRead == -1) return null;
            totalBytes += bytesRead;
            bytesReadIn += bytesRead;
            this.logger.info("The number of bytes read {} and the totalBytes read is {} and full bytes read in for the file are {}", bytesRead, totalBytes, this.bytesReadIn);
        }
        stream.close();
        return ODSUtility.makeChunk(chunkSize, dataSet, (int) part.getStart(), this.fileName);
    }

    /**
     * The point of this doOpen is to prepare the reader to read from the InputStream
     * Establish the s3 client for the next file we encounter; which entails preparing credentials and
     * @throws Exception
     */
    @Override
    protected void doOpen() throws Exception {
        this.currentFileMetaData = this.s3Client.getObjectMetadata(this.amazonS3URI.getBucket(), this.amazonS3URI.getKey());
        partitioner.createParts(this.currentFileMetaData.getContentLength(), this.fileName);
    }

    public AmazonS3 constructClient(){
        AWSCredentials credentials = new BasicAWSCredentials(this.sourceCredential.getUsername(), this.sourceCredential.getSecret());
        return AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(this.amazonS3URI.getRegion())
                .build();
    }

    public String selectRegion(){
        if(this.sourceCredential.getEncryptedSecret().length != 0){
            logger.info("Using encrypted secret to get the region");
            return new String(this.sourceCredential.getEncryptedSecret());
        }else if(this.amazonS3URI.getRegion().length() != 0){
            logger.info("The region is from the s3 URI");
            return this.amazonS3URI.getRegion();
        }else{
            logger.info("Used the default region");
            return Regions.US_EAST_1.toString();
        }
    }

    @Override
    protected void doClose() throws Exception {
    }

    @Override
    public void afterPropertiesSet() throws Exception {
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
     * @return
     */
//    public List<S3ObjectSummary> listObjectFromS3Bucket() {
//        System.out.format("Objects in S3 bucket %s:\n", this.amazonS3URI.getBucket());
//        ListObjectsV2Result result = s3Client.listObjectsV2(this.amazonS3URI.getBucket());
//        objectSummaries = result.getObjectSummaries();
//        return objectSummaries;
//    }
    /**
     * Downloading File Content For a Small File
     * @param keyName
     * @param bucketName
     */
//    public void downloadS3FileContentForSmallFiles(String keyName,String bucketName){
//        S3Object fullObject = s3Client.getObject(new GetObjectRequest(bucketName, keyName));
//        inputStream = fullObject.getObjectContent();
//        fileType = fullObject.getObjectMetadata().getContentType();
//        streamList.add(inputStream);
//        System.out.println("Total Downloaded Content Size from Normal Download: "+
//                fullObject.getObjectMetadata().getContentLength());
//    }

    /**
     * Partial Download For S3
     * Performs Seek Operation
     * Only For Large Files (Greater than 5MB)
     * @param keyName
     * @param bucketName
     */
//    public void downloadS3FileContentForLargeFiles (String keyName, String bucketName) {
//        long bytesDownload = 0;
//        GetObjectRequest rangeObjectRequest;
//        try {
//            System.out.println("Downloading an object");
//            while(bytesDownload<largeFileSize){
//                InputStream localInputStream;
//                System.out.println("Downloaded: "+bytesDownload);
//                if((largeFileSize-bytesDownload)>STANDARD_SIZE) {
//                    rangeObjectRequest = new GetObjectRequest(bucketName, keyName)
//                            .withRange(bytesDownload, STANDARD_SIZE);
//                    bytesDownload += STANDARD_SIZE;
//                }
//                else {
//                    System.out.println("Bytes: "+bytesDownload);
//                    rangeObjectRequest = new GetObjectRequest(bucketName, keyName)
//                            .withRange(bytesDownload, largeFileSize);
//                    bytesDownload += largeFileSize-bytesDownload;
//                }
//                bytesDownload++;
//                S3Object objectPortion = s3Client.getObject(rangeObjectRequest);
//                localInputStream = objectPortion.getObjectContent();
//                streamList.add(localInputStream);
//            }
//        } catch (AmazonServiceException e) {
//            System.err.println(e.getErrorMessage());
//            System.exit(1);
//        } catch (SdkClientException e) {
//            e.printStackTrace();
//        }
//    }
}
