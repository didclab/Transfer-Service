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
    private byte[] data;
    String fileName;
    String basePath;

    public void openConnection() throws IOException {
        URL url = new URL("ftp://user:pass@localhost:2121/dest/tempOutput/firstTransferTest.txt");
//        URL url = new URL(basePath+fileName);
        URLConnection conn = url.openConnection();
//        conn.setDoOutput(true);
//        conn.setDoInput(true);
        this.outputStream = conn.getOutputStream();
    }

    public void writeData() throws IOException {
//        FileUtils.writeByteArrayToFile(new File(basePath + fileName), data,true);
//        PrintStream output = new PrintStream(buffer);

        OutputStream buffer = new BufferedOutputStream(outputStream);
        buffer.write(data);
        ByteArrayOutputStream test = new ByteArrayOutputStream();
        test.writeTo(buffer);
//        output.close();
        buffer.close();
        test.flush();
        test.close();
        outputStream.close();

//        outputStream.write(data);
//        outputStream.flush();
//        outputStream.close();
    }
}
