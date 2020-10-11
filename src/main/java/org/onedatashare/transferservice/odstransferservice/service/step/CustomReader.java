package org.onedatashare.transferservice.odstransferservice.service.step;

import lombok.SneakyThrows;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
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
import java.util.Arrays;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

public class CustomReader<T> extends AbstractItemCountingItemStreamItemReader<DataChunk> implements ResourceAwareItemReaderItemStream<DataChunk>, InitializingBean {

    public static final String DEFAULT_CHARSET = Charset.defaultCharset().name();
    Logger logger = LoggerFactory.getLogger(CustomReader.class);
    //    int current = 0;
    private Resource resource;
    //    private SimpleBinaryBufferedReaderFactory simpleBinaryBufferedReaderFactory;
//    private DefaultBufferedReaderFactory defaultBufferedReaderFactory;
//    private final BufferedReaderFactory bufferedReaderFactory;
//    private InputStream reader;
//    private OutputStream outputStream;
//    private long sizeBuffer;
    //No need now if we use our own ItemReader
    private final String encoding;
//    private boolean noInput;


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
    public void beforeStep(StepExecution stepExecution) throws IOException {
        System.out.println("Before step in custom reader------");
        sBasePath = stepExecution.getJobParameters().getString(SOURCE_BASE_PATH);
        dBasePath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        fName = stepExecution.getJobParameters().getString(FILE_NAME);
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
    }

    //    @AfterStep
//    public void afterStep() throws IOException {
//        reader.close();
//    }
    public CustomReader() {
        this.encoding = DEFAULT_CHARSET;
//        this.bufferedReaderFactory = new DefaultBufferedReaderFactory();
        this.setName(ClassUtils.getShortName(CustomReader.class));
//        this.noInput = false;
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
        byte[] data = new byte[4];
        int flag = this.inputStream.read(data);
        if (flag == -1) {
            return null;
        }
        DataChunk dc = new DataChunk();
        dc.setOutputStream(outputStream);
        dc.setData(data);
        return dc;
    }

    @Override
    protected void doOpen() throws Exception {
        Assert.notNull(this.resource, "Input resource must be set");
//        this.noInput = true;
        if (!this.resource.exists()) {
            //set as exceptions
            logger.warn("Input resource does not exist " + this.resource.getDescription());
        } else if (!this.resource.isReadable()) {
            //set as exceptions
            logger.warn("Input resource is not readable " + this.resource.getDescription());
        } else {
            logger.info("fileName is : " + this.resource.getFilename());
            clientCreateSourceStream(sServerName, sPort, sAccountId, sPass, sBasePath.substring(13 + sAccountId.length() + sPass.length()), fName);
            clientCreateDest(dServerName, dPort, dAccountId, dPass, dBasePath.substring(13 + dAccountId.length() + dPass.length()), fName);
            //STREAM ARE COMING NULL
            System.out.println("inputStream is :" + inputStream);
            System.out.println("outputStream is :" + outputStream);
        }
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
        FTPClient ftpClient = new FTPClient();
        ftpClient.connect(serverName, port);
        ftpClient.login(username, password);
        ftpClient.changeWorkingDirectory(basePath);
        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
        ftpClient.setKeepAlive(true);
        System.out.println("FileName list : " + Arrays.toString(ftpClient.listNames()));
//        System.out.println("source status: " + ftpClient.getStatus());
//        ftpClient.enterLocalPassiveMode();
        ftpClient.completePendingCommand();//If used then FTPClient connection is stuck for infinite time.
        this.inputStream = ftpClient.retrieveFileStream(fName);
    }

    @SneakyThrows
    public void clientCreateDest(String serverName, int port, String username, String password, String basePath, String fName) {
        FTPClient ftpClient = new FTPClient();
        ftpClient.connect(serverName, port);
        ftpClient.login(username, password);
        ftpClient.changeWorkingDirectory(basePath);
        ftpClient.setKeepAlive(true);
        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
        System.out.println("dest status: " + ftpClient.getStatus());
//        ftpClient.completePendingCommand();
        this.outputStream = ftpClient.storeFileStream(fName);
    }
}
