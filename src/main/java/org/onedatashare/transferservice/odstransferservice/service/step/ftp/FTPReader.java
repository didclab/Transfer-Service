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
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfoMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.util.ClassUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

public class FTPReader<T> extends AbstractItemCountingItemStreamItemReader<DataChunk> implements ResourceAwareItemReaderItemStream<DataChunk>, InitializingBean {

    Logger logger = LoggerFactory.getLogger(FTPReader.class);
    public static final String DEFAULT_CHARSET = Charset.defaultCharset().name();
    private final String encoding;
    private Resource resource;

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
        sPass = sAccountIdPass[1];
        fsize = EntityInfoMap.getHm().getOrDefault(fName, 0l);
    }

    public FTPReader() {
        this.encoding = DEFAULT_CHARSET;
        this.setName(ClassUtils.getShortName(FTPReader.class));
    }


    public void setName(String name) {
        this.setExecutionContextName(name);
    }

    @Override
    public void setResource(Resource resource) {
        this.resource = resource;
    }

    public int chunkSize(){
        return SIXTYFOUR_KB < fsize ? SIXTYFOUR_KB : (int) fsize;
    }

    @SneakyThrows
    @Override
    protected DataChunk doRead() {
        if (fsize <= 0) {
            return null;
        }
        int chunkSize = chunkSize();
        byte[] data = new byte[chunkSize];
        fsize -= chunkSize;

        int flag = this.inputStream.read(data);
        if (flag == -1) {
            return null;
        }

        DataChunk dc = new DataChunk();
        dc.setData(data);
        dc.setFileName(fName);
        dc.setSize(chunkSize);
        return dc;
    }


    @Override
    protected void doOpen() {
        clientCreateSourceStream(sServerName, sPort, sAccountId, sPass, sBasePath, fName);
    }

    @Override
    protected void doClose() throws IOException {
        if (inputStream != null) inputStream.close();
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
        FtpFileSystemConfigBuilder.getInstance().setControlEncoding(opts,"UTF-8");
        StaticUserAuthenticator auth = new StaticUserAuthenticator(null, username, password);
        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);
        foSrc = VFS.getManager().resolveFile("ftp://localhost:21/source/" + fName, opts);
        this.inputStream = foSrc.getContent().getInputStream();


        //***GETTING STREAM USING FTP CLIENT***

//        FTPClient ftpClient = new FTPClient();
//        ftpClient.connect(serverName, port);
//        ftpClient.login(username, password);
//        ftpClient.changeWorkingDirectory(basePath);
//        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
//        ftpClient.setKeepAlive(true);
//        ftpClient.enterLocalPassiveMode();
//        ftpClient.setControlKeepAliveTimeout(300);
////        ftpClient.setUseEPSVwithIPv4(true);
////        ftpClient.setControlEncoding("UTF-8");
////        ftpClient.setFileTransferMode(FTP.STREAM_TRANSFER_MODE);
////        ftpClient.completePendingCommand();
//        this.inputStream = ftpClient.retrieveFileStream(fName);

    }
}
