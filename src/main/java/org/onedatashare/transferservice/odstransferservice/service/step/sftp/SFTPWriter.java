package org.onedatashare.transferservice.odstransferservice.service.step.sftp;

import com.jcraft.jsch.*;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;

import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

public class SFTPWriter implements ItemWriter<DataChunk> {

    Logger logger = LoggerFactory.getLogger(SFTPWriter.class);

    String stepName;
    //OutputStream outpuStream;
    private String dBasePath;
    AccountEndpointCredential destCred;

    Session jschSession = null;

    ChannelSftp channelSftp = null;

    public SFTPWriter(AccountEndpointCredential destCred) {
        this.destCred = destCred;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.stepName = stepExecution.getStepName();
        //outpuStream = null;
        dBasePath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
    }

    @AfterStep
    public void afterStep() {
        channelSftp.disconnect();
    }

    public OutputStream getStream(String stepName) {

        if (channelSftp == null || !channelSftp.isConnected()) {
            ftpDest();
            try {
                return channelSftp.put(this.stepName, ChannelSftp.OVERWRITE);
            } catch (SftpException e) {
                logger.error("Unable to retrive outputStream");
                e.printStackTrace();
            }
        }
        logger.info("File already present");
        try {
            return channelSftp.put(this.stepName, ChannelSftp.APPEND);
        } catch (SftpException e) {
            logger.error("Unable to retrive outputStream");
            e.printStackTrace();
        }
        return null;
    }

    public void ftpDest() {
        logger.info("Inside ftpDest for : " + stepName);

        //***GETTING STREAM USING APACHE COMMONS jsch

        JSch jsch = new JSch();
        try {
//            jsch.addIdentity("/home/vishal/.ssh/ods-bastion-dev.pem");
//            jsch.setKnownHosts("/home/vishal/.ssh/known_hosts");

            jsch.addIdentity("randomName", destCred.getSecret().getBytes(), null, null);

            jschSession = jsch.getSession(destCred.getUsername(), destCred.getUri().split(":")[0]);
            jschSession.setConfig("StrictHostKeyChecking", "no");
            jschSession.connect();
            jschSession.setTimeout(10000);
            Channel sftp = jschSession.openChannel("sftp");
            channelSftp = (ChannelSftp) sftp;
            channelSftp.connect();
            try {
                channelSftp.cd(dBasePath);
            } catch (Exception ex) {
                logger.warn("Folder was not present so creating...");
                channelSftp.mkdir(dBasePath);
                channelSftp.cd(dBasePath);
                logger.warn(dBasePath + " folder created.");
            }
            //outpuStream.add(this.stepName);
            logger.info("present directory: ----" + channelSftp.pwd());
        } catch (JSchException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void write(List<? extends DataChunk> items) {
        logger.info("Inside Writer---writing chunk of : " + items.get(0).getFileName());
        OutputStream destination = getStream(this.stepName);
        if (destination != null) {
            try {
                for (DataChunk b : items) {
                    destination.write(b.getData());
                    destination.flush();
                }
            } catch (IOException e) {
                logger.error("Error during writing chunks...exiting");
                e.printStackTrace();
            }
        } else
            logger.error("OutputStream is null....Not able to write : " + items.get(0).getFileName());
    }
}