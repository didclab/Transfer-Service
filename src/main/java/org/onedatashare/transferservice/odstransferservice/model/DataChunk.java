package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;

import java.io.OutputStream;

@Data
public class DataChunk {
    long chunkIdx;
    int startPosition;
    byte[] data;
    String fileName;
    String basePath;
    long size;
}
