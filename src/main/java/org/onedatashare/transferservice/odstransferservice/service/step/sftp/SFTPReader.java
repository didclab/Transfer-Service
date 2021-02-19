package org.onedatashare.transferservice.odstransferservice.service.step.sftp;

import com.jcraft.jsch.*;
import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.service.step.ftp.FTPReader;
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
    int chunckSize;
    AccountEndpointCredential sourceCred;
    FilePartitioner filePartitioner;
    EntityInfo file;

    Session jschSession = null;

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        logger.info("Before step for : " + stepExecution.getStepName());
        sBasePath = stepExecution.getJobParameters().getString(SOURCE_BASE_PATH);
        fName = stepExecution.getStepName();
        filePartitioner.createParts(file.getSize(), stepExecution.getStepName());
    }

    public SFTPReader(AccountEndpointCredential credential, int chunckSize, EntityInfo file) {
        this.file = file;
        this.setExecutionContextName(ClassUtils.getShortName(SFTPReader.class));
        this.chunckSize = chunckSize;
        this.sourceCred = credential;
        this.setName(ClassUtils.getShortName(FTPReader.class));
        this.filePartitioner = new FilePartitioner();
    }

    @Override
    public void setResource(Resource resource) {
    }

    @Override
    protected DataChunk doRead() {
        FilePart filePart = filePartitioner.nextPart();
        byte[] data = new byte[Long.valueOf(filePart.getSize()).intValue()];
        int byteRead = -1;
        try {
            long totalBytes = 0;
            while(totalBytes < filePart.getSize()){
                int bytesRead = this.inputStream.read(data, Long.valueOf(filePart.getStart()).intValue(), Long.valueOf(filePart.getSize()-totalBytes).intValue());
                totalBytes +=bytesRead;
            }
            byteRead = this.inputStream.read(data);
        } catch (IOException ex) {
            logger.error("Unable to read from source");
            ex.printStackTrace();
        }
        if (byteRead == -1) {
            return null;
        }
        DataChunk dc = new DataChunk();
        dc.setStartPosition(Long.valueOf(filePart.getStart()).intValue());
        dc.setChunkIdx(filePart.getPartIdx());
        dc.setSize(byteRead);
        dc.setData(Arrays.copyOf(data, byteRead));
        dc.setFileName(fName);
        return dc;
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

        //***GETTING STREAM USING APACHE COMMONS jsch
        JSch jsch = new JSch();
        try {
//            jsch.addIdentity("/home/vishal/.ssh/ods-bastion-dev.pem");
//            jsch.setKnownHosts("/home/vishal/.ssh/known_hosts");
            jsch.addIdentity("randomName", sourceCred.getSecret().getBytes(), null, null);
            jschSession = jsch.getSession(sourceCred.getUsername(), sourceCred.getUri().split(":")[0]);
            jschSession.setConfig("StrictHostKeyChecking", "no");
            jschSession.connect();
            jschSession.setTimeout(10000);
            Channel sftp = jschSession.openChannel("sftp");
            ChannelSftp channelSftp = (ChannelSftp) sftp;
            channelSftp.connect();
            logger.info("before pwd: ----" + channelSftp.pwd());
            channelSftp.cd(sBasePath);
            logger.info("after pwd: ----" + channelSftp.pwd());
            this.inputStream = channelSftp.get(fName);
        } catch (JSchException e) {
            logger.error("Error in JSch end");
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}