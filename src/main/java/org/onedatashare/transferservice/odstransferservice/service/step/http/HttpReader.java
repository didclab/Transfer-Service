package org.onedatashare.transferservice.odstransferservice.service.step.http;

import org.apache.commons.pool2.ObjectPool;
import org.aspectj.lang.annotation.After;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.model.SetPool;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.pools.HttpConnectionPool;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.*;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Paths;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.SOURCE_BASE_PATH;

public class HttpReader implements SetPool, ItemReader<DataChunk> {

    Logger logger = LoggerFactory.getLogger(HttpReader.class);
    String sBasePath;
    String fileName;
    FilePartitioner filePartitioner;
    EntityInfo fileInfo;
    HttpClient client;
    HttpConnectionPool httpConnectionPool;
    Boolean range;
    AccountEndpointCredential sourceCred;
    Boolean compressable;
    private String uri;


    public HttpReader(EntityInfo fileInfo, AccountEndpointCredential credential) {
//        this.setExecutionContextName(ClassUtils.getShortName(HttpReader.class));
        this.fileInfo = fileInfo;
        this.filePartitioner = new FilePartitioner(fileInfo.getChunkSize());
        this.sourceCred = credential;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) throws IOException, InterruptedException {
        JobParameters params = stepExecution.getJobExecution().getJobParameters();
        this.sBasePath = params.getString(SOURCE_BASE_PATH);
        this.filePartitioner.createParts(this.fileInfo.getSize(), this.fileInfo.getId());
        this.fileName = Paths.get(fileInfo.getId()).getFileName().toString();
        logger.info("Thread={} is reading in fileName={} fileId={}", Thread.currentThread().getName(), this.fileName, this.fileInfo.getId());
        this.uri = sourceCred.getUri() + Paths.get(fileInfo.getPath()).toString();
        this.open();
    }

    @AfterStep
    public ExitStatus afterStep(StepExecution stepExecution) {
        this.close();
        return stepExecution.getExitStatus();
    }


    @Override
    public void setPool(ObjectPool connectionPool) {
        this.httpConnectionPool = (HttpConnectionPool) connectionPool;
    }

    public HttpRequest compressMode(String uri, FilePart filePart, boolean valid) {
        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(uri))
                .setHeader(ODSConstants.ACCEPT_ENCODING, ODSConstants.GZIP)
                .setHeader(valid ? ODSConstants.RANGE : ODSConstants.AccessControlExposeHeaders,
                        valid ? String.format(ODSConstants.byteRange, filePart.getStart(), filePart.getEnd()) : ODSConstants.ContentRange)
                .build();
        return request;
    }

    public HttpRequest rangeMode(String uri, FilePart filePart, boolean valid) {
        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(uri))
                .setHeader(valid ? ODSConstants.RANGE : ODSConstants.AccessControlExposeHeaders,
                        valid ? String.format(ODSConstants.byteRange, filePart.getStart(), filePart.getEnd()) : ODSConstants.ContentRange)
                .build();
        return request;
    }

    @Override
    public DataChunk read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        FilePart filePart = this.filePartitioner.nextPart();
        if (filePart == null) return null;
        HttpRequest request;
        if (this.httpConnectionPool.getCompress() && compressable) {
            request = compressMode(uri, filePart, range);
        } else {
            request = rangeMode(uri, filePart, range);
        }
        HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
//        HttpResponse<byte[]> response = client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).get();
        DataChunk chunk = ODSUtility.makeChunk(response.body().length, response.body(), filePart.getStart(), Long.valueOf(filePart.getPartIdx()).intValue(), this.fileName);
        logger.info(chunk.toString());
        return chunk;
    }

    public void open() throws ItemStreamException {
        try {
            this.client = this.httpConnectionPool.borrowObject();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        String filePath = Paths.get(fileInfo.getPath()).toString();
        uri = sourceCred.getUri() + filePath;
//        HttpRequest request = HttpRequest.newBuilder()
//                .GET()
//                .uri(URI.create(uri)) //make http a string constant as well
//                .setHeader(ODSConstants.ACCEPT_ENCODING, ODSConstants.GZIP)
//                .setHeader(ODSConstants.RANGE, String.format(ODSConstants.byteRange, 0, 1)) //make Range into a string constant as well as bytes
//                .build();
//        HttpResponse<byte[]> response = null;
//        try {
//            response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
//        } catch (IOException | InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//        range = response.statusCode() == 206;
//        compressable = response.headers().allValues(ODSConstants.CONTENT_ENCODING).size() != 0;
    }


    public void close() throws ItemStreamException {
        this.httpConnectionPool.returnObject(this.client);
    }
}