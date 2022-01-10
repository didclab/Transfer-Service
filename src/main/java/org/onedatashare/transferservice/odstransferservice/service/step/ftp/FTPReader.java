package org.onedatashare.transferservice.odstransferservice.service.step.ftp;

import lombok.SneakyThrows;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.model.SetPool;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.pools.FtpConnectionPool;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import java.io.IOException;
import java.io.InputStream;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

public class FTPReader extends AbstractItemCountingItemStreamItemReader<DataChunk> implements SetPool {

    Logger logger = LoggerFactory.getLogger(FTPReader.class);
    InputStream inputStream;
    String sBasePath;
    AccountEndpointCredential sourceCred;
    long fileIdx;
    FilePartitioner partitioner;
    EntityInfo fileInfo;
    private FtpConnectionPool connectionPool;
    private FTPClient client;

    public FTPReader(AccountEndpointCredential credential, EntityInfo file) {
        this.sourceCred = credential;
        fileInfo = file;
        this.partitioner = new FilePartitioner(file.getChunkSize());
        this.setName(ClassUtils.getShortName(FTPReader.class));
    }


    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        logger.info("Before step for : " + stepExecution.getStepName());
        sBasePath = stepExecution.getJobParameters().getString(SOURCE_BASE_PATH);
        sBasePath += fileInfo.getPath();
        fileIdx = 0L;
        this.partitioner.createParts(this.fileInfo.getSize(), fileInfo.getId());
    }

    @AfterStep
    public void afterStep(){
        this.fileIdx = 0;
    }

    public void setName(String name) {
        this.setExecutionContextName(name);
    }


    @SneakyThrows
    @Override
    protected DataChunk doRead() {
        FilePart filePart = this.partitioner.nextPart();
        if(filePart == null) return null;
        byte[] data = new byte[filePart.getSize()];
        int totalBytes = 0;
        while(totalBytes < filePart.getSize()){
            int byteRead = this.inputStream.read(data, totalBytes, filePart.getSize()-totalBytes);
            if (byteRead == -1) return null;
            totalBytes += byteRead;
        }
        DataChunk chunk = ODSUtility.makeChunk(totalBytes, data, filePart.getStart(), (int) filePart.getPartIdx(), filePart.getFileName());
        this.client.setRestartOffset(filePart.getStart());
        this.fileIdx += totalBytes;
        logger.info(chunk.toString());
        return chunk;
    }


    @Override
    protected void doOpen() throws InterruptedException, IOException {
        this.client = this.connectionPool.borrowObject();
        this.inputStream = this.client.retrieveFileStream(this.fileInfo.getId());
        if(this.inputStream == null){
            logger.info("We have NULL inputstream why??");
        }
        logger.info("Finished opening FTPReader");
    }

    @Override
    protected void doClose() {
        try {
            if (inputStream != null) inputStream.close();
        } catch (Exception ex) {
            logger.error("Not able to close the input Stream");
            ex.printStackTrace();
        }
        this.connectionPool.returnObject(this.client);
        logger.info("Finished closing FTPReader");
    }

    @Override
    public void setPool(ObjectPool connectionPool) {
        this.connectionPool = (FtpConnectionPool) connectionPool;
    }
}
