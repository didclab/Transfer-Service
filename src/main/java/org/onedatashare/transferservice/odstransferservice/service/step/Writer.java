package org.onedatashare.transferservice.odstransferservice.service.step;

import com.google.common.primitives.Bytes;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.coyote.http11.Constants.a;

@Component
public class Writer implements ItemWriter<DataChunk> {
    Logger logger = LoggerFactory.getLogger(Writer.class);

    protected FileSystemManager fileSystemManager;
    protected FileSystemOptions fileSystemOptions;

    OutputStream outputStream;
//    String destPath = "Sample destPath";

//    public void getDest(@Value("#{jobParameters['destPath']}") String destPath) {
//        this.destPath = destPath;
//    }
//    @Override
//    public void write(List<? extends byte[]> list) throws Exception {
//        logger.info("Inside Writer"+list);
////        System.out.println("-"+list.get(0).length);
////        System.out.println("--"+list.get(list.size()-1).length);
////        URL url = new URL("ftp://localhost:2121/dest/tempOutput/firstTransferTest.txt");
////        FileObject fileObject = fileSystemManager.resolveFile(url);
////        //Creates the required folders and file
////        fileObject.createFile();
////        outputStream = fileObject.getContent().getOutputStream();
//
//        URL url = new URL("ftp://localhost:2121/dest/tempOutput/firstTransferTest.txt");
//        URLConnection conn = url.openConnection();
//        conn.setDoOutput(true);
//        conn.setDoInput(true);
//        outputStream = conn.getOutputStream();
//
////        byte[] array = list.
////
////        outputStream.write(array);
////        outputStream.flush();
//
//
//        for (byte[] b : list) {
//            outputStream.write(b);
//            outputStream.flush();
//        }
//        outputStream.close();
//    }

    @Override
    public void write(List<? extends DataChunk> list) throws Exception {
        logger.info("Inside Writer"+list);

//        ArrayList<Byte> byteLIst = new ArrayList<Byte>();

        for(DataChunk dc : list){
            dc.openConnection();
            dc.writeData();
//            dc.getOutputStream().write(dc.getData());
//            dc.getOutputStream().flush();
//            dc.getOutputStream().close();
        }
    }

//    public static int[] flatten(List<byte[]> arr) {
//        byte[] result = new byte[arr.size() * arr[0].length];
//        int index = 0;
//        for (int i = 0; i < arr.length; i++) {
//            for (int j = 0; j < arr.length; j++) {
//                result[index++] = arr[i][j];
//            }
//        }
//        return result;
//    }

}
