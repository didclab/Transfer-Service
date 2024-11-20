package org.onedatashare.transferservice.odstransferservice.service.step.sftp;

import com.jcraft.jsch.*;
import com.onedatashare.commonutils.model.credential.AccountEndpointCredential;
import org.apache.commons.pool2.ObjectPool;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.SetPool;
import org.onedatashare.transferservice.odstransferservice.pools.JschSessionPool;
import org.onedatashare.transferservice.odstransferservice.service.InfluxCache;
import org.onedatashare.transferservice.odstransferservice.service.MetricsCollector;
import org.onedatashare.transferservice.odstransferservice.service.step.ODSBaseWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;

public class SFTPWriter extends ODSBaseWriter implements ItemWriter<DataChunk>, SetPool {

    Logger logger = LoggerFactory.getLogger(SFTPWriter.class);

    private String dBasePath;
    AccountEndpointCredential destCred;
    HashMap<String, ChannelSftp> fileToChannel;
    JSch jsch;
    private JschSessionPool connectionPool;
    private Session session;
    private OutputStream destination;


    public SFTPWriter(AccountEndpointCredential destCred, MetricsCollector metricsCollector, InfluxCache influxCache) {
        super(metricsCollector, influxCache);
        fileToChannel = new HashMap<>();
        this.destCred = destCred;
        jsch = new JSch();
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) throws InterruptedException, JSchException, SftpException {
        this.dBasePath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        this.session = this.connectionPool.borrowObject();
        ChannelSftp channelSftp = (ChannelSftp) this.session.openChannel("sftp");
        channelSftp.connect();
        if (this.dBasePath.isEmpty()) {
            this.dBasePath = channelSftp.pwd();
        }
        SftpUtility.createRemoteFolder(channelSftp, this.dBasePath);
        channelSftp.disconnect();
        this.stepExecution = stepExecution;
    }

    @AfterStep
    public ExitStatus afterStep(StepExecution stepExecution) {
        for (ChannelSftp value : fileToChannel.values()) {
            if (value.isConnected()) {
                value.disconnect();
            }
        }
        this.connectionPool.returnObject(this.session);
        return stepExecution.getExitStatus();
    }

    public void establishChannel(String fileName) {
        try {
            ChannelSftp channelSftp = (ChannelSftp) this.session.openChannel("sftp");
            //https://stackoverflow.com/questions/8849240/why-when-i-transfer-a-file-through-sftp-it-takes-longer-than-ftp
            channelSftp.setBulkRequests(Integer.parseInt(this.stepExecution.getJobParameters().getString(ODSConstants.PIPELINING))); //Not very sure if this should be set to the pipelining parameter or not I would assume so
            channelSftp.connect();
            this.cdIntoDir(channelSftp, this.dBasePath);
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
            logger.warn("Failed to cd into directory {}", directory);
            sftpException.printStackTrace();
        }
        return false;
    }

    public OutputStream getStream(String fileName) throws IOException {
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
            logger.warn("Failed creating OutputStream to destPath={}, fileName={}", this.dBasePath, fileName);
            sftpException.printStackTrace();
        }
        throw new IOException();
    }

    @Override
    public void write(Chunk<? extends DataChunk> chunk) throws Exception {
        List<? extends DataChunk> items = chunk.getItems();
//        String fileName = Paths.get(this.dBasePath, items.get(0).getFileName()).toString();
        String fileName = Paths.get(items.get(0).getFileName()).toString();
        try {
            if (this.destination == null) {
                this.destination = getStream(fileName);
            }
            for (DataChunk b : items) {
                logger.info("Current chunk in SFTP Writer " + b.toString());
                destination.write(b.getData());
            }
            destination.flush();
        } catch (IOException ex) {
            this.destination = null;
            createNewSession();
            throw ex;
        }
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
}