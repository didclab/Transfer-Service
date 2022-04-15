package org.onedatashare.transferservice.odstransferservice.service.step.http;

import lombok.SneakyThrows;
import org.apache.commons.pool2.ObjectPool;
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
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.ClassUtils;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.SOURCE_BASE_PATH;

public class HttpReader extends AbstractItemCountingItemStreamItemReader<DataChunk> implements SetPool {

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
    Boolean compress;
    private URI uri;
    private RetryTemplate retryTemplate;


    public HttpReader(EntityInfo fileInfo, AccountEndpointCredential credential) {
        this.setExecutionContextName(ClassUtils.getShortName(HttpReader.class));
        this.fileInfo = fileInfo;
        this.filePartitioner = new FilePartitioner(fileInfo.getChunkSize());
        this.sourceCred = credential;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) throws IOException, InterruptedException {
        JobParameters params = stepExecution.getJobExecution().getJobParameters();
        this.sBasePath = params.getString(SOURCE_BASE_PATH);
        this.filePartitioner.createParts(this.fileInfo.getSize(), this.fileInfo.getId());
        this.compress = this.httpConnectionPool.getCompress();
        this.fileName = fileInfo.getId();
        this.uri =  URI.create(this.sBasePath + fileInfo.getPath()).normalize();
    }

    @Override
    protected DataChunk doRead() throws Exception {
        FilePart filePart = this.filePartitioner.nextPart();
        if(filePart == null) return null;
        return this.retryTemplate.execute((c) -> {
            try {
                byte[] bodyArray;
                if (compress && compressable) {
                    bodyArray = compressMode(uri, filePart, range);
                } else {
                    bodyArray = rangeMode(uri, filePart, range);
                }
                DataChunk chunk = ODSUtility.makeChunk(bodyArray.length, bodyArray, filePart.getStart(), Long.valueOf(filePart.getPartIdx()).intValue(), this.fileName);
                logger.info(chunk.toString());
                return chunk;
            } catch (IOException | InterruptedException ex) {
                throw new IOException();//retry will kick in for both the IOException and InterruptedException.
            }
        });

    }

    @SneakyThrows
    @Override
    protected void doOpen() {
        this.client = this.httpConnectionPool.borrowObject();
        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(uri) //make http a string constant as well
                .setHeader(ODSConstants.ACCEPT_ENCODING, ODSConstants.GZIP)
                .setHeader(ODSConstants.RANGE, String.format(ODSConstants.byteRange,0, 1)) //make Range into a string constant as well as bytes
                .build();
        this.retryTemplate.execute((c) -> {

            HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
            range = response.statusCode() == 206;
            compressable = response.headers().allValues(ODSConstants.CONTENT_ENCODING).size() != 0;
            if(compressable && compress) this.fileName = this.fileName + ".gzip";
            return null;
        });

    }

    @Override
    protected void doClose() {
        this.httpConnectionPool.returnObject(this.client);
    }

    @Override
    public void setPool(ObjectPool connectionPool) {
        this.httpConnectionPool = (HttpConnectionPool) connectionPool;
    }

    public byte[] compressMode(URI uri, FilePart filePart, boolean valid) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(uri)
                .setHeader(ODSConstants.ACCEPT_ENCODING, ODSConstants.GZIP)
                .setHeader(valid ? ODSConstants.RANGE : ODSConstants.AccessControlExposeHeaders,
                        valid ? String.format(ODSConstants.byteRange,filePart.getStart(), filePart.getEnd()) : ODSConstants.ContentRange)
                .build();
        HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
        return response.body();
    }

    public byte[] rangeMode(URI uri, FilePart filePart, boolean valid) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(uri)
                .setHeader(valid ? ODSConstants.RANGE : ODSConstants.AccessControlExposeHeaders,
                        valid ? String.format(ODSConstants.byteRange,filePart.getStart(), filePart.getEnd()) : ODSConstants.ContentRange)
                .build();
        HttpResponse<byte[]> response = this.client.send(request, HttpResponse.BodyHandlers.ofByteArray());
        return response.body();
    }

    public void setRetryTemplate(RetryTemplate retryTemplate) {
        this.retryTemplate = retryTemplate;
    }

}