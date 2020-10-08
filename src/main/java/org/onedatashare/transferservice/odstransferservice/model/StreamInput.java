package org.onedatashare.transferservice.odstransferservice.model;

import lombok.SneakyThrows;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class StreamInput {
    static Logger logger = LoggerFactory.getLogger(StreamInput.class);
    public static FTPClient source;

    @SneakyThrows
    public void clientCreateSource(String serverName, int port, String username, String password, String basePath) {
//        System.out.println(basePath + " ---:--- ");
        FTPClient ftpClient = new FTPClient();
        ftpClient.connect(serverName, port);
        ftpClient.login(username, password);
        ftpClient.changeWorkingDirectory(basePath);
        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
        ftpClient.setKeepAlive(true);
        this.source = ftpClient;
    }



    public static InputStream createInputStream(String fileName) throws IOException {
        return source.retrieveFileStream(fileName);
    }

}
