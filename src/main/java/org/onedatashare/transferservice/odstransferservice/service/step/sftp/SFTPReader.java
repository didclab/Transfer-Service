package org.onedatashare.transferservice.odstransferservice.service.step.sftp;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.onedatashare.commonutils.model.credential.AccountEndpointCredential;
import org.apache.commons.pool2.ObjectPool;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.model.SetPool;
import org.onedatashare.transferservice.odstransferservice.pools.JschSessionPool;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import java.io.IOException;
import java.io.InputStream;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.SOURCE_BASE_PATH;

public class SFTPReader extends AbstractItemCountingItemStreamItemReader<DataChunk> implements SetPool {

    private final int pipeSize;
    Logger logger = LoggerFactory.getLogger(SFTPReader.class);

    InputStream inputStream;
    String sBasePath;
    AccountEndpointCredential sourceCred;
    EntityInfo fileInfo;
    int chunksCreated;
    long fileIdx;
    FilePartitioner filePartitioner;
    private JschSessionPool connectionPool;
    private Session session;
    private ChannelSftp channelSftp;

    public SFTPReader(AccountEndpointCredential credential, EntityInfo file, int pipeSize) {
        this.fileInfo = file;
        this.filePartitioner = new FilePartitioner(file.getChunkSize());
        this.setExecutionContextName(ClassUtils.getShortName(SFTPReader.class));
        this.sourceCred = credential;
        this.setName(ClassUtils.getShortName(SFTPReader.class));
        this.pipeSize = pipeSize;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        logger.info("Before step for : " + stepExecution.getStepName());
        sBasePath = stepExecution.getJobParameters().getString(SOURCE_BASE_PATH);
        chunksCreated = 0;
        fileIdx = 0L;
        this.filePartitioner.createParts(this.fileInfo.getSize(), this.fileInfo.getId());
    }

    @Override
    protected DataChunk doRead() throws IOException {
        FilePart thisChunk = this.filePartitioner.nextPart();
        if (thisChunk == null) return null;
        byte[] data = new byte[thisChunk.getSize()];
        int totalRead = 0;//the total we have read in for this stream
        while (totalRead < thisChunk.getSize()) {
            int bytesRead = this.inputStream.read(data, totalRead, thisChunk.getSize() - totalRead);
            if (bytesRead == -1) return null;
            totalRead += bytesRead;
        }
        DataChunk chunk = ODSUtility.makeChunk(thisChunk.getSize(), data, thisChunk.getStart(), Long.valueOf(thisChunk.getPartIdx()).intValue(), this.fileInfo.getId());
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
        this.channelSftp = (ChannelSftp) this.session.openChannel("sftp");
        this.channelSftp.setBulkRequests(this.pipeSize);
        this.channelSftp.connect();
        this.inputStream = channelSftp.get(fileInfo.getPath());
        //clientCreateSourceStream();
    }

    @Override
    protected void doClose() {
        try {
            if (inputStream != null) inputStream.close();
        } catch (IOException e) {
            logger.info("Exception occurred while closing the stream: {} ", e.getMessage());
        }
        this.channelSftp.disconnect();
        this.connectionPool.returnObject(this.session);
        if (!this.session.isConnected()) {
            this.connectionPool.invalidateAndCreateNewSession(this.session);
        }
    }

    @Override
    public void setPool(ObjectPool connectionPool) {
        this.connectionPool = (JschSessionPool) connectionPool;
    }

}