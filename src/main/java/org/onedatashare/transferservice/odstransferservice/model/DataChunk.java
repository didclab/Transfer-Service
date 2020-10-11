package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;

@Data
public class DataChunk {
    //    private int pageNumber;
    OutputStream outputStream;
    byte[] data;
    String fileName;
    String basePath;
}
