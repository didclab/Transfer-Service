package org.onedatashare.transferservice.odstransferservice.service.step.sftp;

import com.jcraft.jsch.*;
import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.service.step.ftp.FTPReader;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.util.ClassUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

public class SFTPReader<T> extends AbstractItemCountingItemStreamItemReader<DataChunk> implements ResourceAwareItemReaderItemStream<DataChunk>, InitializingBean {

    Logger logger = LoggerFactory.getLogger(SFTPReader.class);

    InputStream inputStream;
    String sBasePath;
    String fName;
    AccountEndpointCredential sourceCred;
    EntityInfo file;
    int chunckSize;
    int chunksCreated;
    long fileIdx;
    FilePartitioner filePartitioner;
    Session jschSession = null;

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        logger.info("Before step for : " + stepExecution.getStepName());
        sBasePath = stepExecution.getJobParameters().getString(SOURCE_BASE_PATH);
        fName = stepExecution.getStepName();
        chunksCreated = 0;
        fileIdx = 0L;
        this.filePartitioner.createParts(this.file.getSize(), this.fName);
    }

    public SFTPReader(AccountEndpointCredential credential, int chunckSize, EntityInfo file) {
        this.file = file;
        this.filePartitioner = new FilePartitioner(chunckSize);
        this.setExecutionContextName(ClassUtils.getShortName(SFTPReader.class));
        this.chunckSize = chunckSize;
        this.sourceCred = credential;
        this.setName(ClassUtils.getShortName(FTPReader.class));
    }

    @Override
    public void setResource(Resource resource) {
    }

    @SneakyThrows
    @Override
    protected DataChunk doRead() {
        FilePart thisChunk = this.filePartitioner.nextPart();
        if (thisChunk == null) return null;
        byte[] data = new byte[thisChunk.getSize()];
        int totalRead = 0;//the total we have read in for this stream
        while (totalRead < thisChunk.getSize()) {
            int bytesRead = 0;
            bytesRead = this.inputStream.read(data, totalRead, thisChunk.getSize() - totalRead);
            if (bytesRead == -1) return null;
            totalRead += bytesRead;
        }
        DataChunk chunk = ODSUtility.makeChunk(thisChunk.getSize(), data, this.fileIdx, this.chunksCreated, this.fName);
        this.fileIdx += totalRead;
        this.chunksCreated++;
        logger.info(chunk.toString());
        return chunk;
    }

    /**
     * Open resources necessary to start reading input.
     *
     * @throws Exception Allows subclasses to throw checked exceptions for interpretation by the framework
     */
    @Override
    protected void doOpen() {
        clientCreateSourceStream();
    }

    @Override
    protected void doClose() {
        try {
            if (inputStream != null) inputStream.close();
        } catch (Exception ex) {
            logger.error("Not able to close the input Stream");
            ex.printStackTrace();
        }
    }

    @Override
    public void afterPropertiesSet() {

    }

    @SneakyThrows
    public void clientCreateSourceStream() {
        logger.info("Inside clientCreateSourceStream for : " + fName);

        JSch jsch = new JSch();
        try {
            ChannelSftp channelSftp = SftpUtility.createConnection(jsch,sourceCred);
            logger.info("before pwd: ----" + channelSftp.pwd());
            if(!sBasePath.isEmpty()){
                channelSftp.cd(sBasePath);
                logger.info("after cd into base path" + channelSftp.pwd());
            }
            this.inputStream = channelSftp.get(file.getId());
        } catch (JSchException e) {
            logger.error("Error in JSch end");
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}