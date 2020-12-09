package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;

import java.io.OutputStream;

@Data
public class DataChunk {
    //    private int pageNumber;
    OutputStream outputStream;
    byte[] data;
    String fileName;
    String basePath;
    long size;
}
