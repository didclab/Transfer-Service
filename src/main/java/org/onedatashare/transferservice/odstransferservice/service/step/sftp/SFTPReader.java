package org.onedatashare.transferservice.odstransferservice.service.step.sftp;

import com.jcraft.jsch.*;
import lombok.SneakyThrows;
import org.apache.commons.pool2.ObjectPool;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.model.SetPool;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.pools.JschSessionPool;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.service.cron.MetricsCollector;
import org.onedatashare.transferservice.odstransferservice.service.step.ftp.FTPReader;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import java.io.IOException;
import java.io.InputStream;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

public class SFTPReader extends AbstractItemCountingItemStreamItemReader<DataChunk> implements SetPool {

    private final int pipeSize;
    Logger logger = LoggerFactory.getLogger(SFTPReader.class);

    InputStream inputStream;
    String sBasePath;
    AccountEndpointCredential sourceCred;
    EntityInfo fileInfo;
    int chunckSize;
    int chunksCreated;
    long fileIdx;
    FilePartitioner filePartitioner;
    private JschSessionPool connectionPool;
    private Session session;
    private ChannelSftp channelSftp;
    private StepExecution stepExecution;
    private MetricsCollector metricsCollector;

    public SFTPReader(AccountEndpointCredential credential, EntityInfo file, int pipeSize) {
        this.fileInfo = file;
        this.filePartitioner = new FilePartitioner(file.getChunkSize());
        this.setExecutionContextName(ClassUtils.getShortName(SFTPReader.class));
        this.sourceCred = credential;
        this.setName(ClassUtils.getShortName(FTPReader.class));
        this.pipeSize = pipeSize;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        logger.info("Before step for : " + stepExecution.getStepName());
        sBasePath = stepExecution.getJobParameters().getString(SOURCE_BASE_PATH);
        chunksCreated = 0;
        fileIdx = 0L;
        this.filePartitioner.createParts(this.fileInfo.getSize(), this.fileInfo.getId());
        this.stepExecution = stepExecution;
        metricsCollector.calculateThroughputAndSave(stepExecution, BYTES_READ, 0L);
    }

    @Override
    protected DataChunk doRead() throws IOException {
        FilePart thisChunk = this.filePartitioner.nextPart();
        if (thisChunk == null) return null;
        byte[] data = new byte[thisChunk.getSize()];
        int totalRead = 0;//the total we have read in for this stream
        while (totalRead < thisChunk.getSize()) {
            int bytesRead = 0;
            bytesRead = this.inputStream.read(data, totalRead, thisChunk.getSize() - totalRead);
            if (bytesRead == -1) return null;
            totalRead += bytesRead;
        }
        DataChunk chunk = ODSUtility.makeChunk(thisChunk.getSize(), data, this.fileIdx, this.chunksCreated, this.fileInfo.getId());
        metricsCollector.calculateThroughputAndSave(stepExecution, BYTES_READ, (long) thisChunk.getSize());
        this.fileIdx += totalRead;
        this.chunksCreated++;
        logger.info(chunk.toString());
        return chunk;
    }

    /**
     * Open resources necessary to start reading input.
     *
     * @throws Exception Allows subclasses to throw checked exceptions for interpretation by the framework
     */
    @Override
    protected void doOpen() throws InterruptedException, JSchException, SftpException {
        this.session = this.connectionPool.borrowObject();
        this.channelSftp = getChannelSftp(session);
        this.channelSftp.setBulkRequests(this.pipeSize);
        this.inputStream = channelSftp.get(fileInfo.getPath());
        //clientCreateSourceStream();
    }

    @Override
    protected void doClose() {
        try {
            if (inputStream != null) inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.channelSftp.disconnect();
        this.connectionPool.returnObject(this.session);
    }

    public ChannelSftp getChannelSftp(Session session) throws JSchException, SftpException {
        if (this.channelSftp == null || !this.channelSftp.isConnected() || this.channelSftp.isClosed()) {
            channelSftp = (ChannelSftp) session.openChannel("sftp");
            channelSftp.connect();
            if(!sBasePath.isEmpty()){
                channelSftp.cd(sBasePath);
                logger.info("after cd into base path" + channelSftp.pwd());
            }
        }
        return channelSftp;
    }

    @SneakyThrows
    public void clientCreateSourceStream() {
        logger.info("Inside clientCreateSourceStream for : " + this.fileInfo.getId());
        JSch jsch = new JSch();
        try {
            ChannelSftp channelSftp = SftpUtility.createConnection(jsch,sourceCred);
            logger.info("before pwd: ----" + channelSftp.pwd());
            if(!sBasePath.isEmpty()){
                channelSftp.cd(sBasePath);
                logger.info("after cd into base path" + channelSftp.pwd());
            }
            this.inputStream = channelSftp.get(fileInfo.getPath());
        } catch (JSchException e) {
            logger.error("Error in JSch end");
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void setPool(ObjectPool connectionPool) {
        this.connectionPool = (JschSessionPool) connectionPool;
    }

    public void setMetricsCollector(MetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
    }
}