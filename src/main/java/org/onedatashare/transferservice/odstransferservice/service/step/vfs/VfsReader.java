//package org.onedatashare.transferservice.odstransferservice.service.step.vfs;
//
//import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
//import org.onedatashare.transferservice.odstransferservice.model.FilePart;
//import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.batch.core.StepExecution;
//import org.springframework.batch.core.annotation.BeforeStep;
//import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
//import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
//import org.springframework.beans.factory.InitializingBean;
//import org.springframework.core.io.Resource;
//import org.springframework.util.ClassUtils;
//
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.nio.channels.FileChannel;
//
//import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;
//
//public class VfsReader extends AbstractItemCountingItemStreamItemReader<DataChunk> implements ResourceAwareItemReaderItemStream<DataChunk>, InitializingBean {
//
//    FileChannel sink;
//    Logger logger = LoggerFactory.getLogger(VfsReader.class);
//    long fsize;
//    FileInputStream inputStream;
//    String sBasePath;
//    String fileName;
//    FilePartitioner filePartitioner;
//
//    public VfsReader() {
//        this.setExecutionContextName(ClassUtils.getShortName(VfsReader.class));
//        this.filePartitioner = new FilePartitioner();
//    }
//
//    @BeforeStep
//    public void beforeStep(StepExecution stepExecution) {
//        logger.info("Before step for : " + stepExecution.getStepName());
//        sBasePath = stepExecution.getJobParameters().getString(SOURCE_BASE_PATH);
//        fileName = stepExecution.getStepName();
//        fsize = StaticVar.getHm().getOrDefault(this.fileName, 0l);
//        filePartitioner.createParts(fsize, fileName);
//    }
//
//    @Override
//    public void setResource(Resource resource) {
//
//    }
//
//    public DataChunk makeChunk(int size, byte[] data, int startPosition) {
//        DataChunk dataChunk = new DataChunk();
//        dataChunk.setStartPosition(startPosition);
//        dataChunk.setFileName(this.fileName);
//        dataChunk.setData(data);
//        dataChunk.setSize(size);
//        return dataChunk;
//    }
//
//    @Override
//    protected DataChunk doRead() {
//        FilePart chunkParameters = this.filePartitioner.nextPart();
//        if (chunkParameters == null) return null;// done as there are no more FileParts in the queue
//        int chunkSize = (int) chunkParameters.getSize();
//        int startPosition = (int) chunkParameters.getStart();
//        ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
//        long totalBytes = 0;
//        while (totalBytes < chunkSize) {
//            int bytesRead = 0;
//            try {
//                bytesRead = this.sink.read(buffer, startPosition);
//            } catch (IOException ex) {
//                logger.error("Unable to read from source");
//                ex.printStackTrace();
//            }
//            if (bytesRead == -1) return null;
//            totalBytes += bytesRead;
//        }
//        buffer.flip();
//        byte[] data = new byte[chunkSize];
//        buffer.get(data, 0, chunkSize);
//        return makeChunk(chunkSize, data, startPosition);
//    }
//
//    @Override
//    protected void doOpen() {
//        try {
//            this.inputStream = new FileInputStream(this.sBasePath + this.fileName);
//        } catch (FileNotFoundException e) {
//            logger.error("Path not found : " + this.sBasePath + this.fileName);
//            e.printStackTrace();
//        }
//        this.sink = this.inputStream.getChannel();
//    }
//
//    @Override
//    protected void doClose() {
//        try {
//            if (inputStream != null) inputStream.close();
//        } catch (Exception ex) {
//            logger.error("Not able to close the input Stream");
//            ex.printStackTrace();
//        }
//    }
//
//    @Override
//    public void afterPropertiesSet() {
//    }
//}
