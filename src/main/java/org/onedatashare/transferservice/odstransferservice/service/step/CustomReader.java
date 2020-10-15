package org.onedatashare.transferservice.odstransferservice.service.step;

import lombok.SneakyThrows;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfoMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.io.*;
import java.nio.charset.Charset;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

public class CustomReader<T> extends AbstractItemCountingItemStreamItemReader<DataChunk> implements ResourceAwareItemReaderItemStream<DataChunk>, InitializingBean {

    public static final String DEFAULT_CHARSET = Charset.defaultCharset().name();
    Logger logger = LoggerFactory.getLogger(CustomReader.class);
    private Resource resource;
    private final String encoding;
    private final int chunk = 4096;


    long fsize;

    OutputStream outputStream;
    InputStream inputStream;
    String sBasePath;
    String dBasePath;
    String fName;
    String sAccountId;
    String dAccountId;
    String sPass;
    String dPass;
    String sServerName;
    int sPort;
    String dServerName;
    int dPort;


    @BeforeStep
    public void beforeStep(StepExecution stepExecution){
        logger.info("Before step for : " + stepExecution.getStepName());
        sBasePath = stepExecution.getJobParameters().getString(SOURCE_BASE_PATH);
        dBasePath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        fName = stepExecution.getStepName();
        String[] sAccountIdPass = stepExecution.getJobParameters().getString(SOURCE_ACCOUNT_ID_PASS).split(":");
        String[] dAccountIdPass = stepExecution.getJobParameters().getString(DESTINATION_ACCOUNT_ID_PASS).split(":");
        String[] sCredential = stepExecution.getJobParameters().getString(SOURCE_CREDENTIAL_ID).split(":");
        String[] dCredential = stepExecution.getJobParameters().getString(DEST_CREDENTIAL_ID).split(":");
        sAccountId = sAccountIdPass[0];
        sPass = sAccountIdPass[1];
        dAccountId = dAccountIdPass[0];
        dPass = dAccountIdPass[1];
        sServerName = sCredential[0];
        sPort = Integer.parseInt(sCredential[1]);
        dServerName = dCredential[0];
        dPort = Integer.parseInt(dCredential[1]);
        fsize = EntityInfoMap.getHm().getOrDefault(fName, 0l);
        System.out.println("Fsize is ----------: " + fsize);
    }

    public CustomReader() {
        this.encoding = DEFAULT_CHARSET;
        this.setName(ClassUtils.getShortName(CustomReader.class));
    }


    public void setName(String name) {
        this.setExecutionContextName(name);
    }

    @Override
    public void setResource(Resource resource) {
        this.resource = resource;
    }

    @SneakyThrows
    @Override
    protected DataChunk doRead() {
        if (fsize <= 0)
            return null;
        byte[] data = new byte[chunk < fsize ? chunk : (int) fsize];
        fsize -= chunk;

        int flag = this.inputStream.read(data);
        if (flag == -1) {
            return null;
        }

        DataChunk dc = new DataChunk();
        dc.setOutputStream(outputStream);
        dc.setData(data);
        dc.setFileName(fName);

        return dc;
    }

    @Override
    protected void doOpen(){
        clientCreateSourceStream(sServerName, sPort, sAccountId, sPass, sBasePath.substring(13 + sAccountId.length() + sPass.length()), fName);
        clientCreateDestStream(dServerName, dPort, dAccountId, dPass, dBasePath.substring(13 + dAccountId.length() + dPass.length()), fName);
    }

    @Override
    protected void doClose() throws Exception {
        if (this.inputStream != null) {
            this.inputStream.close();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
//        Assert.notNull(!this.noInput, "LineMapper is required");
    }

    @SneakyThrows
    public void clientCreateSourceStream(String serverName, int port, String username, String password, String basePath, String fName) {
        logger.info("Inside clientCreateSourceStream for : " + fName);

        FTPClient ftpClient = new FTPClient();
        ftpClient.connect(serverName, port);
        ftpClient.login(username, password);
        ftpClient.changeWorkingDirectory(basePath);
        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
        ftpClient.setKeepAlive(true);
        ftpClient.enterLocalPassiveMode();
//        ftpClient.completePendingCommand();
        this.inputStream = ftpClient.retrieveFileStream(fName);
    }

    @SneakyThrows
    public void clientCreateDestStream(String serverName, int port, String username, String password, String basePath, String fName) {
        logger.info("Inside clientCreateDestStream for : " + fName);
        FTPClient ftpClient = new FTPClient();
        ftpClient.connect(serverName, port);
        ftpClient.login(username, password);
        ftpClient.changeWorkingDirectory(basePath);
        ftpClient.setKeepAlive(true);
        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
        this.outputStream = ftpClient.storeFileStream(fName);
    }
}
