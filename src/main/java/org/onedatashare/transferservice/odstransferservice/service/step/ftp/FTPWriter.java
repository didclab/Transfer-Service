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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;


public class FTPWriter implements ItemWriter<DataChunk> {
    Logger logger = LoggerFactory.getLogger(FTPWriter.class);
    String stepName;
    HashMap<String, OutputStream> drainMap;
    private String dBasePath;
    private String dAccountId;
    private String dServerName;
    private String dPass;
    private int dPort;

    FileObject foDest;

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        drainMap = new HashMap<>();
        this.stepName = stepExecution.getStepName();
        dBasePath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        String[] dAccountIdPass = stepExecution.getJobParameters().getString(DESTINATION_ACCOUNT_ID_PASS).split(":");
        String[] dCredential = stepExecution.getJobParameters().getString(DEST_CREDENTIAL_ID).split(":");
        this.dAccountId = dAccountIdPass[0];
        this.dPass = dAccountIdPass[1];
        this.dServerName = dCredential[0];
        this.dPort = Integer.parseInt(dCredential[1]);
    }

    @AfterStep
    public void afterStep() throws IOException {
        OutputStream steam = drainMap.get(this.stepName);
        steam.close();
    }

    public OutputStream getStream(String stepName) throws IOException {
        if (!drainMap.containsKey(stepName)) {
            ftpDest(this.dServerName, this.dPort, this.dAccountId, this.dPass, this.dBasePath);
        }
        return drainMap.get(stepName);
    }

    public void ftpDest(String serverName, int port, String username, String password, String basePath) throws IOException {
        logger.info("Creating ftpDest---");

        //***GETTING STREAM USING FTPClient

//        FTPClient ftpClient = new FTPClient();
//        ftpClient.connect(serverName, port);
//        ftpClient.login(username, password);
//        ftpClient.makeDirectory(this.dBasePath);
//        ftpClient.changeWorkingDirectory(basePath);
//        ftpClient.setKeepAlive(true);
//        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
////        ftpClient.setAutodetectUTF8(true);
////        ftpClient.setControlKeepAliveTimeout(300);
//        drainMap.put(this.stepName, ftpClient.storeFileStream(this.stepName));


        //***GETTING STREAM USING APACHE COMMONS VFS2

        FileSystemOptions opts = new FileSystemOptions();
        FtpFileSystemConfigBuilder.getInstance().setPassiveMode(opts, true);
        FtpFileSystemConfigBuilder.getInstance().setFileType(opts, FtpFileType.BINARY);
        FtpFileSystemConfigBuilder.getInstance().setAutodetectUtf8(opts, true);
        FtpFileSystemConfigBuilder.getInstance().setControlEncoding(opts,"UTF-8");
        StaticUserAuthenticator auth = new StaticUserAuthenticator(null, username, password);
        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);
        foDest = VFS.getManager().resolveFile("ftp://"+serverName+":"+port+"/"+basePath + this.stepName, opts);
        foDest.createFile();
        drainMap.put(this.stepName,foDest.getContent().getOutputStream());

    }

    public void write(List<? extends DataChunk> list) throws Exception {
        logger.info("Inside Writer---writing chunk of : " + list.get(0).getFileName());
        OutputStream destination = getStream(this.stepName);
        for (DataChunk b : list) {
            destination.write(b.getData());
            destination.flush();
        }
    }
}
