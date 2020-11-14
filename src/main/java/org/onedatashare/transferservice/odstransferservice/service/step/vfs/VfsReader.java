package org.onedatashare.transferservice.odstransferservice.service.step.vfs;

import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.StaticVar;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.util.ClassUtils;

import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

public class VfsReader extends AbstractItemCountingItemStreamItemReader<DataChunk> implements ResourceAwareItemReaderItemStream<DataChunk>, InitializingBean {

    FileChannel sink;
    Logger logger = LoggerFactory.getLogger(VfsReader.class);
    long fsize;
    FileInputStream inputStream;
    String sBasePath;
    String fileName;
    FilePartitioner filePartitioner;

    public VfsReader(){
        this.setName(ClassUtils.getShortName(VfsReader.class));
        this.filePartitioner = new FilePartitioner();
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution){
        sBasePath = stepExecution.getJobParameters().getString(SOURCE_BASE_PATH);
        fileName = stepExecution.getStepName();
        fsize = StaticVar.getHm().getOrDefault(this.fileName, 0l);
        filePartitioner.createParts(fsize, fileName);
    }

    @Override
    public void setResource(Resource resource) {

    }

    public void setName(String name) {
        this.setExecutionContextName(name);
    }

    public DataChunk makeChunk(int size, byte[] data, int startPosition){
        DataChunk dataChunk = new DataChunk();
        dataChunk.setStartPosition(startPosition);
        dataChunk.setFileName(this.fileName);
        dataChunk.setData(data);
        dataChunk.setSize(size);
        return dataChunk;
    }

    @Override
    protected DataChunk doRead() throws Exception {
        FilePart chunkParameters = this.filePartitioner.nextPart();
        if(chunkParameters == null) return null;// done as there are no more FileParts in the queue
        int chunkSize = (int) chunkParameters.getSize();
        int startPosition = (int) chunkParameters.getStart();
        ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
        long totalBytes = 0;
        while(totalBytes < chunkSize){
            int bytesRead = this.sink.read(buffer, startPosition);
            if(bytesRead == -1) return null;
            totalBytes += bytesRead;
        }
        buffer.flip();
        byte[] data = new byte[chunkSize];
        buffer.get(data, 0, chunkSize);
        return makeChunk(chunkSize, data, startPosition);
    }

    @Override
    protected void doOpen() throws Exception {
        this.inputStream = new FileInputStream(this.sBasePath+this.fileName);
        this.sink = this.inputStream.getChannel();
    }

    @Override
    protected void doClose() throws Exception {
        this.inputStream.close();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
    }
}
