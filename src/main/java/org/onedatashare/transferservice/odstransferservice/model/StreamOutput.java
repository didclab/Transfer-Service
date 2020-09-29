package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Data;
import lombok.SneakyThrows;
import org.apache.commons.net.ftp.FTPClient;
import org.onedatashare.transferservice.odstransferservice.service.step.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

@Data
public class StreamOutput {
    Logger logger = LoggerFactory.getLogger(StreamOutput.class);
    static FTPClient destination;

    @SneakyThrows
    public void clientCreateDest() {
        FTPClient ftpClient = new FTPClient();
        ftpClient.connect("localhost", 2121);
        ftpClient.login("user", "pass");
        ftpClient.changeWorkingDirectory("/Documents/");
        ftpClient.setKeepAlive(true);
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
