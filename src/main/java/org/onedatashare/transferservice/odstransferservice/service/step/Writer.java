package org.onedatashare.transferservice.odstransferservice.service.step;


import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;

@Component
public class Writer implements ItemWriter<byte[]> {

    protected FileSystemManager fileSystemManager;
    protected FileSystemOptions fileSystemOptions;

    OutputStream outputStream;
//    String destPath = "Sample destPath";

//    public void getDest(@Value("#{jobParameters['destPath']}") String destPath) {
//        this.destPath = destPath;
//    }
    @Override
    public void write(List<? extends byte[]> list) throws Exception {

//        URL url = new URL("ftp://localhost:2121/dest/tempOutput/firstTransferTest.txt");
//        FileObject fileObject = fileSystemManager.resolveFile(url);
//        //Creates the required folders and file
//        fileObject.createFile();
//        outputStream = fileObject.getContent().getOutputStream();

        URL url = new URL("ftp://localhost:2121/dest/tempOutput/firstTransferTest.txt");
        URLConnection conn = url.openConnection();
        conn.setDoOutput(true);
        conn.setDoInput(true);
        outputStream = conn.getOutputStream();

        for (byte[] b : list) {
            outputStream.write(b);
            outputStream.flush();
        }
        outputStream.close();
    }
}
