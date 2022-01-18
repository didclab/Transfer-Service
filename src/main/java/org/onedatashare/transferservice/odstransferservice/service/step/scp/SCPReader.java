package org.onedatashare.transferservice.odstransferservice.service.step.scp;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
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

import java.io.*;
import java.nio.file.Paths;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;
import static org.onedatashare.transferservice.odstransferservice.service.step.sftp.SftpUtility.*;

public class SCPReader extends AbstractItemCountingItemStreamItemReader<DataChunk> implements SetPool {

    private final FilePartitioner partitioner;
    private final EntityInfo fileInfo;
    private JschSessionPool connectionPool;
    private Session session;
    private InputStream inputStream;
    private Logger logger = LoggerFactory.getLogger(SCPReader.class);
    private String baseBath;
    private Channel channel;
    private final byte[] socketBuffer;
    private OutputStream outputStream;

    public SCPReader(EntityInfo fileInfo){
        this.partitioner = new FilePartitioner(fileInfo.getChunkSize());
        this.fileInfo = fileInfo;
        this.socketBuffer = new byte[1024];
        this.setName(ClassUtils.getShortName(SCPReader.class));
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution){ this.baseBath = stepExecution.getJobParameters().getString(SOURCE_BASE_PATH); }

    /**
     * This prepares everything for the actual "transfer handshake" to take place.
     * @throws JSchException
     * @throws IOException
     * @throws InterruptedException
     */
    public void prepare() throws JSchException, IOException, InterruptedException {
        this.partitioner.createParts(this.fileInfo.getSize(), this.fileInfo.getId());
        this.session = this.connectionPool.borrowObject();
        String path = Paths.get(this.baseBath, this.fileInfo.getPath()).toString();
        logger.info("The command to be used is scp: \t{}",SCP_COMMAND_REMOTE_TO_LOCAL+path);
        this.channel = this.session.openChannel(EXEC);
        ((ChannelExec)channel).setCommand(SCP_COMMAND_REMOTE_TO_LOCAL + path);
        this.inputStream = channel.getInputStream();
        this.outputStream = channel.getOutputStream();
        this.channel.connect();
    }



    @Override
    protected void doOpen() throws Exception {
        logger.info("Inside doOpen SCPReader");
        prepare();
        okAck(this.outputStream, this.socketBuffer);
        if (checkAck(inputStream, logger) != 'C') throw new IOException("ACK for SCPReader failed file: " + this.fileInfo.toString());
        someAck(this.inputStream, this.socketBuffer, logger);
        readFileSize(this.inputStream, this.socketBuffer, logger);
        readFileName(this.inputStream, this.socketBuffer, logger);
        okAck(this.outputStream, this.socketBuffer);
        logger.info("Leaving doOpen SCPReader");
    }

    @Override
    protected DataChunk doRead() throws Exception {
        logger.info("Inside doRead SCPReader");
        FilePart filePart = this.partitioner.nextPart();
        if(filePart == null) return null;
        logger.info(filePart.toString());
        byte[] buffer = new byte[filePart.getSize()];
        int totalBytes = 0;
        while(totalBytes < filePart.getSize()){
            int byteRead = this.inputStream.read(buffer, totalBytes, filePart.getSize()-totalBytes);
            if (byteRead == -1) throw new IOException();
            totalBytes += byteRead;
        }
        DataChunk chunk = ODSUtility.makeChunk(filePart.getSize(), buffer, filePart.getStart(), (int) filePart.getPartIdx(), filePart.getFileName());
        logger.info(chunk.toString());
        return chunk;
    }

    @Override
    protected void doClose() throws Exception {
        if(checkAck(this.inputStream, logger) != 0) throw new IOException("Failed a check ACK in doClose SCPReader");
        okAck(this.outputStream, this.socketBuffer);
        this.inputStream.close();
        this.outputStream.close();
        this.channel.disconnect();
        this.connectionPool.returnObject(this.session);
        logger.debug("Turned off SCPReader");
    }

    @Override
    public void setPool(ObjectPool connectionPool) {
        this.connectionPool = (JschSessionPool) connectionPool;
    }

    public void setName(String name) {
        this.setExecutionContextName(name);
    }
}
