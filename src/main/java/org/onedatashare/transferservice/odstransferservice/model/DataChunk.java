package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;
import lombok.ToString;

@Data
public class DataChunk {
    int chunkIdx;
    long startPosition;
    @ToString.Exclude
    byte[] data;
    String fileName;
    String basePath;
    long size;
}
