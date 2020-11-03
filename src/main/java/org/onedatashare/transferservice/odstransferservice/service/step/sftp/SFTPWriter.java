package org.onedatashare.transferservice.odstransferservice.service.step.sftp;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;

import static org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder.PROXY_STREAM;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

public class SFTPWriter implements ItemWriter<DataChunk> {

    Logger logger = LoggerFactory.getLogger(SFTPWriter.class);

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
            ftpDest();
        }
        return drainMap.get(stepName);
    }

    public void ftpDest() throws IOException {
        logger.info("Inside ftpDest for : " + stepName + " " + dAccountId);
        String tempPass = "";

        //***GETTING STREAM USING APACHE COMMONS VFS2

        FileSystemOptions opts = new FileSystemOptions();

        SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(opts, "no");
        File[] identities = {new File("\\home\\vishal\\ods-bastion-dev.pem")};
        SftpFileSystemConfigBuilder.getInstance().setIdentities(opts, identities);
        SftpFileSystemConfigBuilder.getInstance().setProxyType(opts, PROXY_STREAM);
//        SftpFileSystemConfigBuilder.getInstance().setUserInfo(opts,);
        SftpFileSystemConfigBuilder.getInstance().setConnectTimeoutMillis(opts, 100);

//        StaticUserAuthenticator auth = new StaticUserAuthenticator(null, dAccountId, tempPass);
//        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);
//        foDest = VFS.getManager().resolveFile((String.format("sftp://%s:%s@%s/%s/%s", dAccountId, tempPass, dServerName, dBasePath, this.stepName)));
//        foDest = VFS.getManager().resolveFile("sftp://" + dServerName + "/" + dBasePath + this.stepName, opts);
        foDest = VFS.getManager().resolveFile(String.format("sftp://%s@%s%s/%s", dAccountId, dServerName, dBasePath, this.stepName), opts);
        foDest.createFile();
        drainMap.put(this.stepName, foDest.getContent().getOutputStream());
    }

    @Override
    public void write(List<? extends DataChunk> items) throws Exception {
        logger.info("Inside Writer---writing chunk of : " + items.get(0).getFileName());
        OutputStream destination = getStream(this.stepName);
        for (DataChunk b : items) {
            destination.write(b.getData());
            destination.flush();
        }
    }
}