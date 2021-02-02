package org.onedatashare.transferservice.odstransferservice.utility;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.UploadPartRequest;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;

import java.io.ByteArrayInputStream;

public class ODSUtility {

    public static DataChunk makeChunk(int size, byte[] data, int startPosition, int chunkIdx, String fileName) {
        DataChunk dataChunk = new DataChunk();
        dataChunk.setStartPosition(startPosition);
        dataChunk.setChunkIdx(chunkIdx);
        dataChunk.setFileName(fileName);
        dataChunk.setData(data);
        dataChunk.setSize(size);
        return dataChunk;
    }

    public static UploadPartRequest makePartRequest(DataChunk dataChunk, String bucketName, String uploadId, String key, boolean lastPart){
        UploadPartRequest uploadPartRequest = new UploadPartRequest();
        uploadPartRequest.setInputStream(new ByteArrayInputStream(dataChunk.getData()));
        uploadPartRequest.setBucketName(bucketName);
        uploadPartRequest.withLastPart(lastPart);
        uploadPartRequest.setUploadId(uploadId);
        uploadPartRequest.setKey(key);
        uploadPartRequest.setPartNumber(Long.valueOf(dataChunk.getChunkIdx()).intValue()+1); //by default we start from chunks 0-N but AWS SDK must have 1-10000 so we just add 1
        uploadPartRequest.setPartSize(dataChunk.getSize());
        return uploadPartRequest;
    }


}
