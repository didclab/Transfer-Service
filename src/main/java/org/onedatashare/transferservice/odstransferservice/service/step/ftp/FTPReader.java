package org.onedatashare.transferservice.odstransferservice.service.step.ftp;

import lombok.SneakyThrows;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.ftp.FtpFileType;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
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
import java.util.Arrays;
import java.util.HashMap;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

public class FTPReader<T> extends AbstractItemCountingItemStreamItemReader<DataChunk> implements ResourceAwareItemReaderItemStream<DataChunk>, InitializingBean {

    Logger logger = LoggerFactory.getLogger(FTPReader.class);

    InputStream inputStream;
    String sBasePath;
    String fName;
    int chunckSize;
    AccountEndpointCredential sourceCred;
    FileObject foSrc;

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        logger.info("Before step for : " + stepExecution.getStepName());
        sBasePath = stepExecution.getJobParameters().getString(SOURCE_BASE_PATH);
        fName = stepExecution.getStepName();
    }

    public FTPReader(AccountEndpointCredential credential, int chunckSize) {
        logger.info("Inside FTPReader constructor");
        this.chunckSize = chunckSize;
        this.sourceCred = credential;
        this.setName(ClassUtils.getShortName(FTPReader.class));
    }


    public void setName(String name) {
        logger.info("Setting context name");
        this.setExecutionContextName(name);
    }

    @Override
    public void setResource(Resource resource) {
    }

    @SneakyThrows
    @Override
    protected DataChunk doRead() {
        byte[] data = new byte[this.chunckSize];
        int byteRead = this.inputStream.read(data);
        if (byteRead == -1) {
            return null;
        }

        DataChunk dc = new DataChunk();
        dc.setData(Arrays.copyOf(data, byteRead));
        dc.setSize(byteRead);
        dc.setFileName(fName);
        return dc;
    }


    @Override
    protected void doOpen() {
        logger.info("Insided doOpen");
        clientCreateSourceStream(sBasePath, fName);
    }

    @Override
    protected void doClose() {
        logger.info("Inside doClose");
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
    public void clientCreateSourceStream(String basePath, String fName) {
        logger.info("Inside clientCreateSourceStream for : " + fName + " ");

        //***GETTING STREAM USING APACHE COMMONS VFS2
        FileSystemOptions opts = new FileSystemOptions();
        FtpFileSystemConfigBuilder.getInstance().setPassiveMode(opts, true);
        FtpFileSystemConfigBuilder.getInstance().setFileType(opts, FtpFileType.BINARY);
        FtpFileSystemConfigBuilder.getInstance().setAutodetectUtf8(opts, true);
        FtpFileSystemConfigBuilder.getInstance().setControlEncoding(opts, "UTF-8");
        StaticUserAuthenticator auth = new StaticUserAuthenticator(null, this.sourceCred.getUsername(), this.sourceCred.getSecret());
        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);
        this.foSrc = VFS.getManager().resolveFile("ftp://" + this.sourceCred.getUri() + "/" + basePath + fName, opts);
        this.inputStream = foSrc.getContent().getInputStream();
    }
}
