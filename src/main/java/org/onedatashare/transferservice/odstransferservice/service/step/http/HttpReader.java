package org.onedatashare.transferservice.odstransferservice.service.step.http;

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
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.http.HttpStatus;
import org.springframework.retry.support.RetryTemplate;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Paths;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.SOURCE_BASE_PATH;

public class HttpReader implements SetPool, ItemReader<DataChunk> {

    String sBasePath;
    String fileName;
    FilePartitioner filePartitioner;
    EntityInfo fileInfo;
    HttpClient client;
    HttpConnectionPool httpConnectionPool;
    Boolean range;
    AccountEndpointCredential sourceCred;
    private URI uri;
    Logger logger;
    private RetryTemplate retryTemplate;


    public HttpReader(EntityInfo fileInfo, AccountEndpointCredential credential) {
//        this.setExecutionContextName(ClassUtils.getShortName(HttpReader.class));
        this.fileInfo = fileInfo;
        this.filePartitioner = new FilePartitioner(fileInfo.getChunkSize());
        this.sourceCred = credential;
        this.range = true;
        this.logger = LoggerFactory.getLogger(HttpReader.class);
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) throws IOException, InterruptedException {
        JobParameters params = stepExecution.getJobExecution().getJobParameters();
        this.sBasePath = params.getString(SOURCE_BASE_PATH);
        this.filePartitioner.createParts(this.fileInfo.getSize(), this.fileInfo.getId());
        this.fileName = Paths.get(fileInfo.getId()).getFileName().toString();
        this.uri = URI.create(sourceCred.getUri() + Paths.get(fileInfo.getPath()));
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

    public HttpRequest compressMode(FilePart filePart, boolean valid) {
        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(this.uri)
                .setHeader(ODSConstants.ACCEPT_ENCODING, ODSConstants.GZIP)
                .setHeader(valid ? ODSConstants.RANGE : ODSConstants.AccessControlExposeHeaders,
                        valid ? String.format(ODSConstants.byteRange, filePart.getStart(), filePart.getEnd()) : ODSConstants.ContentRange)
                .build();
        return request;
    }

    public HttpRequest rangeMode(FilePart filePart, boolean valid) {
        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(this.uri)
                .setHeader(valid ? ODSConstants.RANGE : ODSConstants.AccessControlExposeHeaders,
                        valid ? String.format(ODSConstants.byteRange, filePart.getStart(), filePart.getEnd()) : ODSConstants.ContentRange)
                .build();
        return request;
    }

    @Override
    public DataChunk read() {
        FilePart filePart = this.filePartitioner.nextPart();
        if (filePart == null) return null;
        HttpRequest request;
        if (this.httpConnectionPool.getCompress()) {
            request = compressMode(filePart, this.range);
        } else {
            request = rangeMode(filePart, this.range);
        }

        try {
            return this.retryTemplate.execute(context -> {
                try {
                    HttpResponse<byte[]> response = this.client.send(request, HttpResponse.BodyHandlers.ofByteArray());
                    byte[] data =response.body();
                    return ODSUtility.makeChunk(data.length, data, filePart.getStart(), Long.valueOf(filePart.getPartIdx()).intValue(), this.fileName);
                } catch (IOException | InterruptedException e) {
                    logger.error("Http request of FilePart: {} failed with error: {}", filePart, e.getMessage());
                    throw e;
                }
            });
        } catch (Exception e) {
            return null;
        }

    }

    public void open() throws ItemStreamException {
        try {
            this.client = this.httpConnectionPool.borrowObject();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    public void close() throws ItemStreamException {
        this.httpConnectionPool.returnObject(this.client);
    }

    public void setRetry(RetryTemplate retryTemplate) {
        this.retryTemplate = retryTemplate;
    }
}