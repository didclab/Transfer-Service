package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;
import lombok.SneakyThrows;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

@Data
public class StreamOutput {
    Logger logger = LoggerFactory.getLogger(StreamOutput.class);
    static FTPClient destination;

    @SneakyThrows
    public void clientCreateDest(String serverName, int port, String username, String password, String basePath) {
//        System.out.println(basePath + " ---:--- ");
        FTPClient ftpClient = new FTPClient();
        ftpClient.connect(serverName, port);
        ftpClient.login(username, password);
        ftpClient.changeWorkingDirectory(basePath);
        ftpClient.setKeepAlive(true);
        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
        this.destination = ftpClient;
    }
    public static OutputStream outputStream;

    public static void setOutputStream(OutputStream os) {
        outputStream = os;
    }

    public static OutputStream getOutputStream() {
        return outputStream;
    }

    public static void createOutputStream(String fileName) throws IOException {
        outputStream = destination.storeFileStream(fileName);
    }
}
