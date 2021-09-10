package org.onedatashare.transferservice.odstransferservice.service.step.sftp;

import com.jcraft.jsch.*;
import org.apache.commons.pool2.ObjectPool;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;

import org.onedatashare.transferservice.odstransferservice.model.SetPool;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.pools.JschSessionPool;
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

public class SFTPWriter implements ItemWriter<DataChunk>, SetPool {

    Logger logger = LoggerFactory.getLogger(SFTPWriter.class);

    private String dBasePath;
    AccountEndpointCredential destCred;
    HashMap<String, ChannelSftp> fileToChannel;
    JSch jsch;
    private JschSessionPool connectionPool;
    private Session session;

    public SFTPWriter(AccountEndpointCredential destCred) {
        fileToChannel = new HashMap<>();
        this.destCred = destCred;
        jsch = new JSch();
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) throws InterruptedException {
        this.dBasePath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        this.session = this.connectionPool.borrowObject();
    }

    @AfterStep
    public void afterStep() {
        for(ChannelSftp value: fileToChannel.values()){
            if(!value.isConnected()){
                value.disconnect();
            }
        }
        this.connectionPool.returnObject(this.session);
    }

    public void establishChannel(String stepName){
        try {
            ChannelSftp channelSftp = SftpUtility.createConnection(jsch, destCred);
            assert channelSftp != null;
            if(!cdIntoDir(channelSftp, dBasePath)){
                SftpUtility.mkdir(channelSftp, dBasePath);
            }
            if(fileToChannel.containsKey(stepName)){
                fileToChannel.remove(stepName);
            }
            fileToChannel.put(stepName, channelSftp);
        } catch (JSchException e) {
            e.printStackTrace();
        }
    }

    public boolean cdIntoDir(ChannelSftp channelSftp, String directory){
        try {
            channelSftp.cd(directory);
            return true;
        } catch (SftpException sftpException) {
            logger.warn("Could not cd into the directory we might have made moohoo");
            sftpException.printStackTrace();
        }
        return false;
    }

    public OutputStream getStream(String stepName) {
        boolean appendMode = false;
        if(!fileToChannel.containsKey(stepName)){
            establishChannel(stepName);
        }else if(fileToChannel.get(stepName).isClosed() || !fileToChannel.get(stepName).isConnected()){
            fileToChannel.remove(stepName);
            appendMode = true;
            establishChannel(stepName);
        }
        ChannelSftp channelSftp = this.fileToChannel.get(stepName);
        try {
            if(appendMode){
                return channelSftp.put(stepName, ChannelSftp.APPEND);
            }
            return channelSftp.put(stepName, ChannelSftp.OVERWRITE);
        } catch (SftpException sftpException) {
            logger.warn("We failed getting the OuputStream to a file :(");
            sftpException.printStackTrace();
        }
        return null;
    }

    @Override
    public void write(List<? extends DataChunk> items) {
        String fileName = items.get(0).getFileName();
        OutputStream destination = getStream(items.get(0).getFileName());
        if(destination == null){
            logger.error("OutputStream is null....Not able to write : " + items.get(0).getFileName());
            establishChannel(fileName);
        }else{
            try {
                for (DataChunk b : items) {
                    logger.info("Current chunk in SFTP Writer " + b.toString());
                    destination.write(b.getData());
                    destination.flush();
                }
            } catch (IOException e) {
                logger.error("Error during writing chunks...exiting");
                e.printStackTrace();
            }
        }
    }

    @Override
    public void setPool(ObjectPool connectionPool) {
        this.connectionPool = (JschSessionPool) connectionPool;
    }
}