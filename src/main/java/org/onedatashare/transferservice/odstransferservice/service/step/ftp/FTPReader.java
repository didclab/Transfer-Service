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
    OutputStream outputStream;
    InputStream inputStream;
    String sBasePath;
    String fName;
    String sAccountId;
    String sPass;
    String sServerName;
    int sPort;
    String dBasePath;
    String dAccountId;
    String dServerName;
    String dPass;
    int dPort;


    //***VFS2 SETTING

//    FileObject foSrc;
//    FileObject foDest;
//    FileSystemOptions opts;


    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        logger.info("Before step for : " + stepExecution.getStepName());
        sBasePath = stepExecution.getJobParameters().getString(SOURCE_BASE_PATH);
        dBasePath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        fName = stepExecution.getStepName();
        String[] sAccountIdPass = stepExecution.getJobParameters().getString(SOURCE_ACCOUNT_ID_PASS).split(":");
        String[] dAccountIdPass = stepExecution.getJobParameters().getString(DESTINATION_ACCOUNT_ID_PASS).split(":");
        String[] dCredential = stepExecution.getJobParameters().getString(DEST_CREDENTIAL_ID).split(":");
        String[] sCredential = stepExecution.getJobParameters().getString(SOURCE_CREDENTIAL_ID).split(":");
        sAccountId = sAccountIdPass[0];
        sServerName = sCredential[0];
        sPort = Integer.parseInt(sCredential[1]);
        sPass = sAccountIdPass[1];
        fsize = EntityInfoMap.getHm().getOrDefault(fName, 0l);
        dAccountId = dAccountIdPass[0];
        dPass = dAccountIdPass[1];
        dServerName = dCredential[0];
        dPort = Integer.parseInt(dCredential[1]);

     //**VFS2 SETTING***

//        this.opts = new FileSystemOptions();
//        FtpFileSystemConfigBuilder.getInstance().setPassiveMode(this.opts, true);
//        FtpFileSystemConfigBuilder.getInstance().setFileType(this.opts, FtpFileType.BINARY);
//        FtpFileSystemConfigBuilder.getInstance().setAutodetectUtf8(opts, true);
//        FtpFileSystemConfigBuilder.getInstance().setControlEncoding(this.opts,"UTF-8");
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

    @SneakyThrows
    @Override
    protected DataChunk doRead() {
        if (fsize <= 0) {
            return null;
        }
        byte[] data = new byte[SIXTYFOUR_KB < fsize ? SIXTYFOUR_KB : (int) fsize];
        fsize -= SIXTYFOUR_KB;

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
    protected void doOpen() {
        clientCreateSourceStream(sServerName, sPort, sAccountId, sPass, sBasePath, fName);
        clientCreateDestStream(dServerName, dPort, dAccountId, dPass, dBasePath, fName);
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

//        StaticUserAuthenticator auth = new StaticUserAuthenticator(null, username, password);
//        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);
//        foSrc = VFS.getManager().resolveFile("ftp://localhost:21/source/" + fName, opts);
//        this.inputStream = foSrc.getContent().getInputStream();


        //***GETTING STREAM USING FTP CLIENT***

        FTPClient ftpClient = new FTPClient();
        ftpClient.connect(serverName, port);
        ftpClient.login(username, password);
        ftpClient.changeWorkingDirectory(basePath);
        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
        ftpClient.setKeepAlive(true);
        ftpClient.enterLocalPassiveMode();
//        ftpClient.setUseEPSVwithIPv4(true);
//        ftpClient.setAutodetectUTF8(true);
//        ftpClient.setControlEncoding("UTF-8");
//        ftpClient.setControlKeepAliveTimeout(300);
//        ftpClient.setFileTransferMode(FTP.STREAM_TRANSFER_MODE);
//        ftpClient.completePendingCommand();
        this.inputStream = ftpClient.retrieveFileStream(fName);
    }

    @SneakyThrows
    public void clientCreateDestStream(String serverName, int port, String username, String password, String basePath, String fName) {
        logger.info("Inside clientCreateDestStream for : " + fName + " " + username);

        //***GETTING STREAM USING APACHE COMMONS VFS2

//        StaticUserAuthenticator auth = new StaticUserAuthenticator(null, username, password);
//        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);
//        foDest = VFS.getManager().resolveFile("ftp://localhost:21/dest/tempOutput/"+fName, opts);
////        foDest = VFS.getManager().resolveFile("ftp://3.137.215.170/home/ftpuser/hello/" + fName, opts);
//        foDest.createFile();
//        this.outputStream = foDest.getContent().getOutputStream();


        //***GETTING STREAM USING FTP CLIENT***

        FTPClient ftpClient = new FTPClient();
        ftpClient.connect(serverName, port);
        ftpClient.login(username, password);
        ftpClient.changeWorkingDirectory(basePath);
        ftpClient.setKeepAlive(true);
        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
//        ftpClient.setUseEPSVwithIPv4(true);
//        ftpClient.enterLocalPassiveMode();
//        ftpClient.setAutodetectUTF8(true);
//        ftpClient.setControlEncoding("UTF-8");
//        ftpClient.setControlKeepAliveTimeout(300);
//        ftpClient.setFileTransferMode(FTP.STREAM_TRANSFER_MODE);
//        ftpClient.completePendingCommand();
        this.outputStream = ftpClient.storeFileStream(fName);
    }

}
