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
import org.springframework.retry.support.RetryTemplate;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;

public class SFTPWriter implements ItemWriter<DataChunk>, SetPool {

    private final int pipeSize;
    Logger logger = LoggerFactory.getLogger(SFTPWriter.class);

    private String dBasePath;
    AccountEndpointCredential destCred;
    HashMap<String, ChannelSftp> fileToChannel;
    JSch jsch;
    private JschSessionPool connectionPool;
    private Session session;
    private OutputStream destination;
    private RetryTemplate retryTemplate;

    public SFTPWriter(AccountEndpointCredential destCred, int pipeSize) {
        fileToChannel = new HashMap<>();
        this.destCred = destCred;
        jsch = new JSch();
        this.pipeSize = pipeSize;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) throws InterruptedException, JSchException, SftpException {
        this.dBasePath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        this.session = this.connectionPool.borrowObject();
        ChannelSftp channelSftp = (ChannelSftp) this.session.openChannel("sftp");
        channelSftp.connect();
        if(this.dBasePath.isEmpty()){
            this.dBasePath = channelSftp.pwd();
        }
        SftpUtility.createRemoteFolder(channelSftp, this.dBasePath);
        channelSftp.disconnect();
    }

    @AfterStep
    public void afterStep() {
        for (ChannelSftp value : fileToChannel.values()) {
            if (value.isConnected()) {
                value.disconnect();
            }
        }
        this.connectionPool.returnObject(this.session);
    }

    public void establishChannel(String fileName) {
        try {
            ChannelSftp channelSftp = (ChannelSftp) this.session.openChannel("sftp");
            channelSftp.setBulkRequests(this.pipeSize); //Not very sure if this should be set to the pipelining parameter or not I would assume so
            channelSftp.connect();
            this.cdIntoDir(channelSftp, this.dBasePath);
//            ChannelSftp channelSftp = SftpUtility.createConnection(jsch, destCred);
//            if(!cdIntoDir(channelSftp, dBasePath)){
//                SftpUtility.mkdir(channelSftp, dBasePath);
//            }
            fileToChannel.put(fileName, channelSftp);
        } catch (JSchException e) {
            e.printStackTrace();
        }
    }

    public boolean cdIntoDir(ChannelSftp channelSftp, String directory) {
        try {
            channelSftp.cd(directory);
            return true;
        } catch (SftpException sftpException) {
            logger.warn("Could not cd into the directory we might have made moohoo");
            sftpException.printStackTrace();
        }
        return false;
    }

    public OutputStream getStream(String fileName) {
        boolean appendMode = false;
        if (!fileToChannel.containsKey(fileName)) {
            establishChannel(fileName);
        } else if (fileToChannel.get(fileName).isClosed() || !fileToChannel.get(fileName).isConnected()) {
            fileToChannel.remove(fileName);
            appendMode = true;
            establishChannel(fileName);
        }
        ChannelSftp channelSftp = this.fileToChannel.get(fileName);
        try {
            if (appendMode) {
                return channelSftp.put(fileName, ChannelSftp.APPEND);
            }
            return channelSftp.put(fileName, ChannelSftp.OVERWRITE);
        } catch (SftpException sftpException) {
            logger.warn("We failed getting the OuputStream to a file :(");
            sftpException.printStackTrace();
        }
        return null;
    }

    @Override
    public void write(List<? extends DataChunk> items) throws Exception {
//        String fileName = Paths.get(this.dBasePath, items.get(0).getFileName()).toString();
        String fileName = Paths.get(items.get(0).getFileName()).toString();
        List<? extends DataChunk> itemsToProcess = items;
        this.retryTemplate.execute((c) -> {
            try {
                if (this.destination == null) {
                    this.destination = getStream(fileName);
                }
                for (DataChunk b : itemsToProcess) {
                    logger.info("Current chunk in SFTP Writer " + b.toString());
                    destination.write(b.getData());
                }
                destination.flush();
            } catch (IOException ex) {
                this.destination = null;
                createNewSession();
                throw ex;
            }
            return null;
        });
    }

    protected void createNewSession() throws InterruptedException {
        if (!this.session.isConnected()) {
            this.connectionPool.invalidateAndCreateNewSession(this.session);
            this.session = this.connectionPool.borrowObject();
        }
    }

    @Override
    public void setPool(ObjectPool connectionPool) {
        this.connectionPool = (JschSessionPool) connectionPool;
    }

    public void setRetryTemplate(RetryTemplate retryTemplate) {
        this.retryTemplate = retryTemplate;
    }
}