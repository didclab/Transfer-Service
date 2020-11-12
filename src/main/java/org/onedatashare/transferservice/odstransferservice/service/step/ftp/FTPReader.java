package org.onedatashare.transferservice.odstransferservice.service.step.ftp;

import lombok.SneakyThrows;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.ftp.FtpFileType;
import org.onedatashare.transferservice.odstransferservice.controller.TransferController;
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
import org.springframework.util.ClassUtils;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

public class FTPReader<T> extends AbstractItemCountingItemStreamItemReader<DataChunk> implements ResourceAwareItemReaderItemStream<DataChunk>, InitializingBean {

    Logger logger = LoggerFactory.getLogger(FTPReader.class);

    long fsize;
    InputStream inputStream;
    String sBasePath;
    String fName;
    String sAccountId;
    String sPass;
    String sServerName;
    int sPort;


    //***VFS2 SETTING
    FileObject foSrc;

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        logger.info("Before step for : " + stepExecution.getStepName());
        sBasePath = stepExecution.getJobParameters().getString(SOURCE_BASE_PATH);
        fName = stepExecution.getStepName();
        String[] sAccountIdPass = stepExecution.getJobParameters().getString(SOURCE_ACCOUNT_ID_PASS).split(":");
        String[] sCredential = stepExecution.getJobParameters().getString(SOURCE_CREDENTIAL_ID).split(":");
        sAccountId = sAccountIdPass[0];
        sServerName = sCredential[0];
        sPort = Integer.parseInt(sCredential[1]);
//        sPass = sAccountIdPass[1];
        this.sPass = EntityInfoMap.sPass;
        fsize = EntityInfoMap.getHm().getOrDefault(fName, 0l);
    }

    public FTPReader() {
        this.setName(ClassUtils.getShortName(FTPReader.class));
    }


    public void setName(String name) {
        this.setExecutionContextName(name);
    }

    @Override
    public void setResource(Resource resource) {
    }

    @SneakyThrows
    @Override
    protected DataChunk doRead() {
        byte[] data = new byte[SIXTYFOUR_KB];
        int byteRead = this.inputStream.read(data);
        if (byteRead == -1) {
            return null;
        }

        DataChunk dc = new DataChunk();
        dc.setData(Arrays.copyOf(data, byteRead));
        dc.setFileName(fName);
        return dc;
    }


    @Override
    protected void doOpen() {
        clientCreateSourceStream(sServerName, sPort, sAccountId, sPass, sBasePath, fName);
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
    public void afterPropertiesSet() throws Exception {
    }

    @SneakyThrows
    public void clientCreateSourceStream(String serverName, int port, String username, String password, String basePath, String fName) {
        logger.info("Inside clientCreateSourceStream for : " + fName + " " + username);

        //***GETTING STREAM USING APACHE COMMONS VFS2

        FileSystemOptions opts = new FileSystemOptions();
        FtpFileSystemConfigBuilder.getInstance().setPassiveMode(opts, true);
        FtpFileSystemConfigBuilder.getInstance().setFileType(opts, FtpFileType.BINARY);
        FtpFileSystemConfigBuilder.getInstance().setAutodetectUtf8(opts, true);
        FtpFileSystemConfigBuilder.getInstance().setControlEncoding(opts, "UTF-8");
        StaticUserAuthenticator auth = new StaticUserAuthenticator(null, username, password);
        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);
        foSrc = VFS.getManager().resolveFile("ftp://" + serverName + ":" + port + "/" + basePath + fName, opts);
        this.inputStream = foSrc.getContent().getInputStream();
    }
}
