package org.onedatashare.transferservice.odstransferservice.service.step.ftp;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.ftp.FtpFileType;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.StaticVar;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
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
    AccountEndpointCredential destCred;

    FileObject foDest;

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        drainMap = new HashMap<>();
        this.stepName = stepExecution.getStepName();
        dBasePath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        String[] dAccountIdPass = stepExecution.getJobParameters().getString(DESTINATION_ACCOUNT_ID_PASS).split(":");
        String[] dCredential = stepExecution.getJobParameters().getString(DEST_CREDENTIAL_ID).split(":");
        this.dAccountId = dAccountIdPass[0];
        this.dPass = StaticVar.dPass;
        this.destCred = (AccountEndpointCredential) StaticVar.getDestCred();
        this.dServerName = dCredential[0];
        this.dPort = Integer.parseInt(dCredential[1]);
    }

    @AfterStep
    public void afterStep() {
        OutputStream outputStream = drainMap.get(this.stepName);
        try {
            if (outputStream != null) outputStream.close();
        } catch (Exception ex) {
            logger.error("Not able to close the input Stream");
            ex.printStackTrace();
        }
    }

    public OutputStream getStream(String stepName) {
        if (!drainMap.containsKey(stepName)) {
            ftpDest(this.dServerName, this.dPort, this.destCred.getUsername(), this.destCred.getSecret(), this.dBasePath);
        }
        return drainMap.get(stepName);
    }

    public void ftpDest(String serverName, int port, String username, String password, String basePath) {
        logger.info("Creating ftpDest for :" + this.stepName);

        //***GETTING STREAM USING APACHE COMMONS VFS2
        try {
            FileSystemOptions opts = new FileSystemOptions();
            FtpFileSystemConfigBuilder.getInstance().setPassiveMode(opts, true);
            FtpFileSystemConfigBuilder.getInstance().setFileType(opts, FtpFileType.BINARY);
            FtpFileSystemConfigBuilder.getInstance().setAutodetectUtf8(opts, true);
            FtpFileSystemConfigBuilder.getInstance().setControlEncoding(opts, "UTF-8");
            StaticUserAuthenticator auth = new StaticUserAuthenticator(null, this.destCred.getUsername(), this.destCred.getSecret());
            DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);
            foDest = VFS.getManager().resolveFile("ftp://" + this.destCred.getUri() + "/" + basePath + this.stepName, opts);
            foDest.createFile();
            drainMap.put(this.stepName, foDest.getContent().getOutputStream());
        } catch (Exception ex) {
            logger.error("Error in setting ftp connection...");
            ex.printStackTrace();
        }
    }

    public void write(List<? extends DataChunk> list) {
        logger.info("Inside Writer---writing chunk of : " + list.get(0).getFileName());
        OutputStream destination = getStream(this.stepName);
        try {
            for (DataChunk b : list) {
                destination.write(b.getData());
                destination.flush();
            }
        } catch (IOException e) {
            logger.error("Error during writing chunks...exiting");
            e.printStackTrace();
        }
    }
}
