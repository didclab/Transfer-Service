package org.onedatashare.transferservice.odstransferservice.service.step.http;

import com.fasterxml.jackson.databind.util.ClassUtil;
import lombok.SneakyThrows;
import org.apache.commons.pool2.ObjectPool;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpGet;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.model.SetPool;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.pools.JschSessionPool;
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
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;

import java.nio.channels.Channels;

import java.nio.channels.ReadableByteChannel;
import java.time.Duration;


import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.SOURCE_BASE_PATH;
import org.onedatashare.transferservice.odstransferservice.pools.HttpConnectionPool;

public class HttpReader<T> extends AbstractItemCountingItemStreamItemReader<DataChunk> implements SetPool {

    ReadableByteChannel channel;
    Logger logger = LoggerFactory.getLogger(HttpReader.class);
    int chunkSize;
    FileInputStream fileInputStream;
    String sBasePath;
    String fileName;
    FilePartitioner filePartitioner;
    EntityInfo fileInfo;
    ByteBuffer buffer;
    HttpClient client;
    HttpConnectionPool httpConnectionPool;
    Boolean range;


    public HttpReader(EntityInfo fileInfo, int chunkSize) {
        this.setExecutionContextName(ClassUtils.getShortName(HttpReader.class));
        this.fileInfo = fileInfo;
        this.filePartitioner = new FilePartitioner(chunkSize);
        this.chunkSize = chunkSize;
        buffer = ByteBuffer.allocate(this.chunkSize);
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) throws IOException, InterruptedException {
        logger.info("Before step for : " + stepExecution.getStepName());
        JobParameters params = stepExecution.getJobExecution().getJobParameters();
        this.sBasePath = params.getString(SOURCE_BASE_PATH);
        this.filePartitioner.createParts(this.fileInfo.getSize(), this.fileInfo.getId());
        this.fileName = stepExecution.getStepName();

        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create("https://"+ fileInfo.getPath() + fileInfo.getId())) //make http a string constant as well
                .setHeader("Range", "bytes=0-10") //make Range into a string constant as well as bytes
                .build();
        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(20))
                .build();
        HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
        if(response.statusCode() == 206) this.range = true;
        else this.range = false;
    }

    @Override
    protected DataChunk doRead() throws IOException, InterruptedException {
        FilePart filePart = this.filePartitioner.nextPart();
        if(filePart == null) return null;
        byte[] bodyArray;
        if(range){
            HttpRequest request = HttpRequest.newBuilder()
                    .GET()
                    .uri(URI.create("https://" + fileInfo.getPath() + fileInfo.getId())) //make http a string constant as well
                    .setHeader("Range", "bytes="+filePart.getStart()+"-"+(filePart.getStart()+chunkSize)) //make Range into a string constant as well as bytes
                    .build();
            HttpResponse<byte[]> response = this.client.send(request, HttpResponse.BodyHandlers.ofByteArray());
            bodyArray = response.body();
        }
        else{
            HttpRequest fullRequest = HttpRequest.newBuilder()
                    .GET()
                    .uri(URI.create("https://" + fileInfo.getPath() + fileInfo.getId())) //make http a string constant as well
                    .setHeader("Access-Control-Expose-Headers", "Content-Range")
                    .build();
            HttpResponse<byte[]> fullResponse = this.client.send(fullRequest, HttpResponse.BodyHandlers.ofByteArray());
            bodyArray = fullResponse.body();
        }




//
//
//        logger.info("headers: " + request.headers());
//        logger.info("status code is: " + response.statusCode());
//        logger.info("header is: " + response.headers());
//        logger.info("body byte length: " + bodyArray.length);
//        logger.info("file getStart: " + filePart.getStart());
//        logger.info("file get Idx: " + Long.valueOf(filePart.getPartIdx()).intValue());
//        logger.info("file name:" + this.fileName);

        return ODSUtility.makeChunk(bodyArray.length, bodyArray, filePart.getStart(), Long.valueOf(filePart.getPartIdx()).intValue(), this.fileName);
    }

    @SneakyThrows
    @Override
    protected void doOpen() throws InterruptedException {
        logger.info("current httpConnectionPoll size: " + this.httpConnectionPool.getSize());
        this.client = this.httpConnectionPool.borrowObject();
    }

    @Override
    protected void doClose() {
//        try{
//            if(fileInputStream != null) fileInputStream.close();
//            channel.close();
//        }catch (Exception e) {
//            logger.error("Not able to close the input Stream");
//            e.printStackTrace();
//        }
    }

    @Override
    public void setPool(ObjectPool connectionPool) {
        this.httpConnectionPool = (HttpConnectionPool) connectionPool;
        logger.info("I set the poll");
    }
}