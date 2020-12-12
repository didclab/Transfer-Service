package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;

@Data
public class DataChunk {
    int startPosition;
    byte[] data;
    String fileName;
    String basePath;
    long size;
}
