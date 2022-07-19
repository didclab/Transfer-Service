package org.onedatashare.transferservice.odstransferservice.utility;

import com.amazonaws.services.s3.model.UploadPartRequest;
import com.dropbox.core.DbxRequestConfig;
import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.springframework.beans.factory.annotation.Value;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.HashSet;

public class ODSUtility {

    @Value("${dropbox.identifier}")
    private static String odsClientID = "OneDataShare-DIDCLab";

    public static DbxRequestConfig dbxRequestConfig = DbxRequestConfig.newBuilder(odsClientID).build();

    public static DataChunk makeChunk(int size, byte[] data, int startPosition, int chunkIdx, String fileName) {
        DataChunk dataChunk = new DataChunk();
        dataChunk.setStartPosition(startPosition);
        dataChunk.setChunkIdx(chunkIdx);
        dataChunk.setFileName(fileName);
        dataChunk.setData(data);
        dataChunk.setSize(size);
        return dataChunk;
    }
    public static DataChunk makeChunk(long size, byte[] data, long startPosition, int chunkIdx, String fileName) {
        DataChunk dataChunk = new DataChunk();
        dataChunk.setStartPosition(startPosition);
        dataChunk.setChunkIdx(chunkIdx);
        dataChunk.setFileName(fileName);
        dataChunk.setData(data);
        dataChunk.setSize(size);
        return dataChunk;
    }


    public static UploadPartRequest makePartRequest(DataChunk dataChunk, String bucketName, String uploadId, String key, boolean lastPart) {
        UploadPartRequest uploadPartRequest = new UploadPartRequest();
        uploadPartRequest.setInputStream(new ByteArrayInputStream(dataChunk.getData()));
        uploadPartRequest.setBucketName(bucketName);
        uploadPartRequest.withLastPart(lastPart);
        uploadPartRequest.setUploadId(uploadId);
        uploadPartRequest.setKey(key);
//        uploadPartRequest.setFileOffset(dataChunk.getStartPosition());
        uploadPartRequest.setPartNumber(dataChunk.getChunkIdx()+1); //by default we start from chunks 0-N but AWS SDK must have 1-10000 so we just add 1
        uploadPartRequest.setPartSize(dataChunk.getSize());
        return uploadPartRequest;
    }

    public static final EndpointType[] SEEKABLE_PROTOCOLS = new EndpointType[]{EndpointType.s3, EndpointType.vfs, EndpointType.gdrive, EndpointType.dropbox, EndpointType.http};

    public static final HashSet<EndpointType> fullyOptimizableProtocols = new HashSet<EndpointType>(Arrays.asList(SEEKABLE_PROTOCOLS));
}
