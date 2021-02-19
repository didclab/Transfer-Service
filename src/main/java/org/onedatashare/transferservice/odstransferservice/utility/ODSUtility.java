package org.onedatashare.transferservice.odstransferservice.utility;

import com.amazonaws.services.s3.model.UploadPartRequest;
import edu.emory.mathcs.backport.java.util.Arrays;
import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;

import java.io.ByteArrayInputStream;
import java.util.HashSet;

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

    public static UploadPartRequest makePartRequest(DataChunk dataChunk, String bucketName, String uploadId, String key, boolean lastPart) {
        UploadPartRequest uploadPartRequest = new UploadPartRequest();
        uploadPartRequest.setInputStream(new ByteArrayInputStream(dataChunk.getData()));
        uploadPartRequest.setBucketName(bucketName);
        uploadPartRequest.withLastPart(lastPart);
        uploadPartRequest.setUploadId(uploadId);
        uploadPartRequest.setKey(key);
        uploadPartRequest.setPartNumber(Long.valueOf(dataChunk.getChunkIdx()).intValue() + 1); //by default we start from chunks 0-N but AWS SDK must have 1-10000 so we just add 1
        uploadPartRequest.setPartSize(dataChunk.getSize());
        return uploadPartRequest;
    }

    public static final EndpointType[] SEEKABLE_PROTOCOLS = new EndpointType[]{EndpointType.s3, EndpointType.vfs};
    public static final EndpointType[] NON_SEEKABLE_PROTOCOLS = new EndpointType[]{};

    public static final HashSet<EndpointType> fullyOptimizableProtocols = new HashSet<EndpointType>(Arrays.asList(SEEKABLE_PROTOCOLS));
    public static final HashSet<EndpointType> notFullyOptimizableProtcols = new HashSet<>();
}
