package org.onedatashare.transferservice.odstransferservice.service.step.sftp;

import lombok.SneakyThrows;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

public class SFTPReader<T> extends AbstractItemCountingItemStreamItemReader<DataChunk> implements ResourceAwareItemReaderItemStream<DataChunk>, InitializingBean {

    Logger logger = LoggerFactory.getLogger(SFTPReader.class);

    long fileSize;
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
        sPass = sAccountIdPass[1];
        fileSize = EntityInfoMap.getHm().getOrDefault(fName, 0l);
    }

    public SFTPReader() {
        this.setExecutionContextName(ClassUtils.getShortName(SFTPReader.class));
    }

    @Override
    public void setResource(Resource resource) {

    }

    @Override
    protected DataChunk doRead() {
        byte[] data = new byte[SIXTYFOUR_KB];
        int byteRead = -1;
        try {
            byteRead = this.inputStream.read(data);
        } catch (IOException ex) {
            logger.error("Unable to read from source");
            ex.printStackTrace();
        }
        if (byteRead == -1) {
            return null;
        }

        DataChunk dc = new DataChunk();
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
        logger.info("Inside clientCreateSourceStream for : " + fName + " " + sAccountId);


        //***GETTING STREAM USING APACHE COMMONS VFS2

        FileSystemOptions opts = new FileSystemOptions();

        SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(opts, "no");
        File[] identities = {new File("PEM FILE PATH")};
        SftpFileSystemConfigBuilder.getInstance().setIdentities(opts, identities);
        SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(opts, true);
        SftpFileSystemConfigBuilder.getInstance().setConnectTimeoutMillis(opts, 100);

        StaticUserAuthenticator auth = new StaticUserAuthenticator(null, sAccountId, sPass);
        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);
        foSrc = VFS.getManager().resolveFile("sftp://" + sServerName + ":" + sPort + "/" + sBasePath + fName, opts);
        this.inputStream = foSrc.getContent().getInputStream();

    }
}