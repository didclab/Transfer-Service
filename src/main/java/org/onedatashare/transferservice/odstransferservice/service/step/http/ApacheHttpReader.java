package org.onedatashare.transferservice.odstransferservice.service.step.http;

import org.apache.commons.pool2.ObjectPool;

import org.apache.http.*;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpPipeliningClient;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.model.SetPool;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.pools.ApacheHttpConnectionPool;
import org.onedatashare.transferservice.odstransferservice.pools.HttpConnectionPool;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterChunk;
import org.springframework.batch.core.annotation.AfterChunkError;
import org.springframework.batch.core.annotation.AfterRead;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Future;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;


public class ApacheHttpReader extends AbstractItemCountingItemStreamItemReader<DataChunk> {
    Logger logger = LoggerFactory.getLogger(ApacheHttpReader.class);
    String sBasePath;
    String fileName;
    FilePartitioner filePartitioner;
    EntityInfo fileInfo;
    CloseableHttpPipeliningClient client;
    ApacheHttpConnectionPool apacheHttpConnectionPool;
    Boolean range;
    AccountEndpointCredential sourceCred;
    Boolean compressable;
    Boolean compress;
    HttpHost host;
    long httpPipeSize;
    private String uri;

    public ApacheHttpReader(EntityInfo fileInfo, AccountEndpointCredential credential) {
        this.setExecutionContextName(ClassUtils.getShortName(ApacheHttpReader.class));
        this.fileInfo = fileInfo;
        this.filePartitioner = new FilePartitioner(fileInfo.getChunkSize());
        this.sourceCred = credential;
    }


    public void setPool(ApacheHttpConnectionPool connectionPool) {
        this.apacheHttpConnectionPool = connectionPool;
    }


    @BeforeStep
    public void beforeStep(StepExecution stepExecution) throws IOException, InterruptedException {
        JobParameters params = stepExecution.getJobExecution().getJobParameters();
        this.httpPipeSize = params.getLong(HTTP_PIPELINING);
        this.sBasePath = params.getString(SOURCE_BASE_PATH);
        this.filePartitioner.createParts(this.fileInfo.getSize(), this.fileInfo.getId());
        this.compress = this.apacheHttpConnectionPool.getCompress();
        this.fileName = fileInfo.getId();
        this.uri = sourceCred.getUri();
    }


    @Override
    protected DataChunk doRead() throws Exception {
        int requestCount=0;
        List<HttpRequest> requests = new ArrayList<>();
        long startPosition=-1,partIndex=-1,resultChunkSize=0;
        while(requestCount < httpPipeSize){
            FilePart filePart = this.filePartitioner.nextPart();
            if (filePart == null)
                break;
            //Retrieving the least start position
            if(startPosition==-1){
                startPosition = filePart.getStart();
            }else{
                startPosition = filePart.getStart() < startPosition ? filePart.getStart() : startPosition;
            }

            if(partIndex==-1){
                partIndex = filePart.getPartIdx();
            }else{
                partIndex = filePart.getPartIdx() < partIndex ? filePart.getPartIdx() : partIndex;
            }

            //Adding all the request chunks size
            resultChunkSize+=filePart.getSize();
            HttpGet get = new HttpGet(fileInfo.getPath());
            get.setProtocolVersion(HttpVersion.HTTP_1_1);
            get.setHeader(range ? ODSConstants.RANGE : ODSConstants.AccessControlExposeHeaders,
                    range ? String.format(ODSConstants.byteRange,filePart.getStart(), filePart.getEnd()) : ODSConstants.ContentRange);
            if(compress && compressable) {
                get.setHeader(ODSConstants.ACCEPT_ENCODING, ODSConstants.GZIP);
            }
            requests.add(get);
            requestCount++;
        }
        if(requests.size()==0){
            return null;
        }
        Future<List<HttpResponse>> future = client.execute(host, requests, null);

        List<HttpResponse> responses = future.get();
        DataChunk chunk = null;
        try(ByteArrayOutputStream outputStream = new ByteArrayOutputStream()){
            for (HttpResponse response : responses) {
                response.getEntity().getContent().transferTo(outputStream);
            }
            chunk = ODSUtility.makeChunk(resultChunkSize, outputStream.toByteArray(), startPosition, (int)partIndex, this.fileName);
        }

        logger.info(chunk.toString());
        return chunk;
    }

    @Override
    protected void doOpen() throws Exception {
        this.client = this.apacheHttpConnectionPool.borrowObject();
        this.host = HttpHost.create(uri);
        HttpGet[] requests = { new HttpGet(fileInfo.getPath()) };
        requests[0].setHeader(ODSConstants.ACCEPT_ENCODING, ODSConstants.GZIP);
        requests[0].setHeader(ODSConstants.RANGE, String.format(ODSConstants.byteRange,0, 1));
        Future<List<HttpResponse>> future = client.execute(host, Arrays.<HttpRequest>asList(requests), null);
        List<HttpResponse> responses = future.get();
        responses.forEach((response) -> {
            this.range = response.getStatusLine().getStatusCode() == 206;
            compressable = response.containsHeader(ODSConstants.CONTENT_ENCODING);
        });
        if(compressable && compress) this.fileName = this.fileName + ".gzip";


    }

    @Override
    protected void doClose() throws Exception {
        this.apacheHttpConnectionPool.returnObject(this.client);
        this.client=null;
    }


}
