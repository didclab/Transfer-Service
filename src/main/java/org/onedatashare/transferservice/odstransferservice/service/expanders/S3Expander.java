package org.onedatashare.transferservice.odstransferservice.service.expanders;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.lang.StringUtils;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.model.credential.EndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.DestinationChunkSize;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;

@Component
public class S3Expander extends DestinationChunkSize implements FileExpander {

    AmazonS3 s3Client;
    String[] regionAndBucket;

    @Override
    public void createClient(EndpointCredential cred) {
        AccountEndpointCredential credential = EndpointCredential.getAccountCredential(cred);
        this.regionAndBucket = credential.getUri().split(":::");
        AWSCredentials credentials = new BasicAWSCredentials(credential.getUsername(), credential.getSecret());
        this.s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(regionAndBucket[0])
                .build();
    }

    @Override
    public List<EntityInfo> expandedFileSystem(List<EntityInfo> userSelectedResources, String basePath) {
        List<EntityInfo> traversedFiles = new LinkedList<>();
        //trim leading forward slashes from base path (s3 doesn't recognise it as root)
        basePath = StringUtils.stripStart(basePath, "/");
        if(userSelectedResources.isEmpty()){//expand the whole bucket relative to the basePath
            ListObjectsV2Result result = this.s3Client.listObjectsV2(createSkeletonPerResource(basePath));
            traversedFiles.addAll(convertV2ResultToEntityInfoList(result));
        }
        for(EntityInfo userSelectedResource: userSelectedResources){
            //we have a folder/prefix for s3
            if (userSelectedResource.getPath().endsWith("/")){
                ListObjectsV2Request req = createSkeletonPerResource(userSelectedResource.getPath());
                ListObjectsV2Result res = this.s3Client.listObjectsV2(req);
                for(S3ObjectSummary obj : res.getObjectSummaries()){
                    if(obj.getKey().endsWith("/")) continue;
                    EntityInfo entityInfo = new EntityInfo();
                    entityInfo.setId(obj.getKey());
                    entityInfo.setPath(obj.getKey());
                    entityInfo.setSize(obj.getSize());
                    traversedFiles.add(entityInfo);
                }
                // the case where the user selected a file
            } else if(this.s3Client.doesObjectExist(this.regionAndBucket[1], userSelectedResource.getPath())){
                ObjectMetadata metadata = this.s3Client.getObjectMetadata(this.regionAndBucket[1],userSelectedResource.getPath());
                userSelectedResource.setSize(metadata.getContentLength());
                traversedFiles.add(userSelectedResource);
            }
        }

        return traversedFiles;
    }

    public List<EntityInfo> convertV2ResultToEntityInfoList(ListObjectsV2Result result){
        List<EntityInfo> traversedFiles = new LinkedList<>();
        for(S3ObjectSummary fileInfo : result.getObjectSummaries()){
            EntityInfo entityInfo = new EntityInfo();
            entityInfo.setId(fileInfo.getKey());
            entityInfo.setPath(fileInfo.getKey());
            entityInfo.setSize(fileInfo.getSize());
            traversedFiles.add(entityInfo);
        }
        return traversedFiles;
    }

    public ListObjectsV2Request createSkeletonPerResource(String path){
        if(path.isEmpty()){
            return new ListObjectsV2Request()
                    .withBucketName(regionAndBucket[1]);
        }else{
            return new ListObjectsV2Request()
                    .withBucketName(regionAndBucket[1])
                    .withPrefix(path);
        }
    }

    @Override
    public List<EntityInfo> destinationChunkSize(List<EntityInfo> expandedFiles, String basePath, Integer userChunkSize) {
        for (EntityInfo fileInfo : expandedFiles) {
            if(userChunkSize < 5000000){
                fileInfo.setChunkSize(10000000);
            }else{
                fileInfo.setChunkSize(userChunkSize);
            }
        }
        return expandedFiles;
    }
}
