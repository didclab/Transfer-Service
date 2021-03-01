package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;

import java.io.OutputStream;

@Data
public class DataChunk {
    int chunkIdx;
    long startPosition;
    byte[] data;
    String fileName;
    String basePath;
    long size;
}
