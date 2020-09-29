package org.onedatashare.transferservice.odstransferservice.model;

import lombok.SneakyThrows;
import org.apache.commons.net.ftp.FTPClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class StreamInput {
    Logger logger = LoggerFactory.getLogger(StreamOutput.class);
    public static FTPClient source;

    @SneakyThrows
    public void clientCreateSource() {
        FTPClient ftpClient = new FTPClient();
        ftpClient.connect("localhost", 2121);
        ftpClient.login("user", "pass");
        ftpClient.changeWorkingDirectory("/Downloads/outputTransfer");
        ftpClient.setKeepAlive(true);
        this.source = ftpClient;
    }


    public static InputStream createInputStream(String fileName) throws IOException {
        return source.retrieveFileStream(fileName);
    }

}
