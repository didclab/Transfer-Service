package org.onedatashare.transferservice.odstransferservice.service.step.scp;

import com.jcraft.jsch.*;
import lombok.SneakyThrows;
import org.apache.commons.pool2.ObjectPool;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.SetPool;
import org.onedatashare.transferservice.odstransferservice.pools.JschSessionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.AfterWrite;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.annotation.BeforeWrite;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.ItemWriter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;
import static org.onedatashare.transferservice.odstransferservice.service.step.sftp.SftpUtility.*;
import static org.onedatashare.transferservice.odstransferservice.service.step.sftp.SftpUtility.okAck;

public class SCPWriter implements ItemWriter<DataChunk>, SetPool {

    private final EntityInfo fileInfo;
    Logger logger = LoggerFactory.getLogger(SCPWriter.class);

    private String dBasePath;
    private JschSessionPool connectionPool;
    private Session session;
    private ChannelExec scpChannel;
    private OutputStream outputStream;
    private InputStream inputStream;
    private byte[] socketBuffer;

    public SCPWriter(EntityInfo fileInfo){
        this.socketBuffer = new byte[1024];
        this.fileInfo = fileInfo;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) throws InterruptedException, JSchException, IOException {
        logger.info("Before Step in SCPWriter");
        this.dBasePath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
    }

    @BeforeWrite
    public void beforeWrite(List<DataChunk> items){
        this.open(items.get(0).getFileName(), this.fileInfo.getSize());
    }

    @AfterWrite
    public void afterWrite(List<DataChunk> items) throws IOException {
        okAck(this.outputStream, this.socketBuffer);
        if(this.outputStream != null){
            this.outputStream.close();
        }
        if(this.inputStream != null){
            this.inputStream.close();
        }
        this.scpChannel.disconnect();
        this.connectionPool.returnObject(this.session);
        logger.info("Shut Down SCPWriter ");
    }

    @Override
    public void write(List<? extends DataChunk> items) throws IOException, JSchException {
        logger.info("Inside write SCPWriter");
        for (DataChunk b : items) {
            outputStream.write(b.getData());
            logger.info("Wrote {}", b);
        }
        outputStream.flush();
        items = null;
    }

    @Override
    public void setPool(ObjectPool connectionPool) {
        this.connectionPool = (JschSessionPool) connectionPool;
    }

    @SneakyThrows
    public void open(String fileName, long fileSize) {
        String fullPath = Paths.get(this.dBasePath,fileName).toString();
        this.session = this.connectionPool.borrowObject();
        mkdirSCP(session, this.dBasePath, logger);
        this.scpChannel = (ChannelExec) this.session.openChannel("exec");
        scpChannel.setCommand(SCP_COMMAND_LOCAL_TO_REMOTE + fullPath);
        this.outputStream = scpChannel.getOutputStream();
        this.inputStream = scpChannel.getInputStream();
        this.scpChannel.connect();
        if(checkAck(this.inputStream,logger) != 0) throw new IOException("ACK for SCPReader failed file: " + fileName);
        sendFileSize(this.outputStream, fileName, fileSize);
        if(checkAck(inputStream, logger) != 0) throw new IOException("ACK for SCPReader failed file: " + fileName);
    }

}