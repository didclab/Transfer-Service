package org.onedatashare.transferservice.odstransferservice.service.step.http;

import com.fasterxml.jackson.databind.util.ClassUtil;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpGet;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.service.step.vfs.VfsReader;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;

import java.nio.channels.Channels;

import java.nio.channels.ReadableByteChannel;



import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.SOURCE_BASE_PATH;
import org.onedatashare.transferservice.odstransferservice.pools.HttpConnectionPool;

public class HttpReader<T> extends AbstractItemCountingItemStreamItemReader<DataChunk>{

    ReadableByteChannel channel;
    Logger logger = LoggerFactory.getLogger(HttpReader.class);
    int chunkSize;
    FileInputStream fileInputStream;
    String sBasePath;
    String fileName;
    FilePartitioner filePartitioner;
    EntityInfo fileInfo;
    ByteBuffer buffer;
//    FileOutputStream fos;
//    FileChannel fileChannel;


    public HttpReader(EntityInfo fileInfo, int chunkSize) {
        this.setExecutionContextName(ClassUtils.getShortName(HttpReader.class));
        this.fileInfo = fileInfo;
        this.filePartitioner = new FilePartitioner(chunkSize);
        this.chunkSize = chunkSize;
        buffer = ByteBuffer.allocate(this.chunkSize);
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        logger.info("Before step for : " + stepExecution.getStepName());
        JobParameters params = stepExecution.getJobExecution().getJobParameters();
        this.sBasePath = params.getString(SOURCE_BASE_PATH);
        this.filePartitioner.createParts(this.fileInfo.getSize(), this.fileInfo.getId());
    }

    @Override
    protected DataChunk doRead() throws IOException {
        // using range not filePart
        FilePart filePart = this.filePartitioner.nextPart();
        if(filePart == null) return null;
        int totalByte = 0;
        while(totalByte < filePart.getSize()) {
            int byteRead = 0;
            byteRead = this.channel.read(buffer);
            if(byteRead == -1) return null;
            totalByte += byteRead;
        }
        buffer.flip();
        byte[] data = new byte[filePart.getSize()];
        buffer.get(data, 0, totalByte);
        buffer.clear();
        return ODSUtility.makeChunk(totalByte, data, filePart.getStart(), Long.valueOf(filePart.getPartIdx()).intValue(), this.fileName);


        /**
         * this is another way to download file to specific path by FOS

         fos.getChannel().transferFrom(channel, 0, filePart.getSize());

         */

    }

    @Override
    protected void doOpen() throws IOException, InterruptedException, URISyntaxException {
//        this.client = this.httpConnectionPool.borrowObject();
//        this.client.executor();
        URL url = new URL("https://" + fileInfo.getPath() + fileInfo.getId());
        System.out.println("this is the url: "+ url.toString());
        this.channel = Channels.newChannel(url.openStream());

        /**
         * this is another way to download file to specific path by FOS


           URL url = new URL("https://" + fileInfo.getPath() + fileInfo.getId());
           this.channel = Channels.newChannel(url.openStream());
           this.fileOutputStream = new FileOutputStream("aa");
           fos = fileOutputStream.getChannel();

         */
    }

    @Override
    protected void doClose() {
        try{
            if(fileInputStream != null) fileInputStream.close();
            channel.close();
        }catch (Exception e) {
            logger.error("Not able to close the input Stream");
            e.printStackTrace();
        }
    }
}
