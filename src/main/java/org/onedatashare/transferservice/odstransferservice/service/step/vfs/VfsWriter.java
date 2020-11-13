package org.onedatashare.transferservice.odstransferservice.service.step.vfs;

import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.HashMap;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;

public class VfsWriter implements ItemWriter<DataChunk> {
    Logger logger = LoggerFactory.getLogger(VfsWriter.class);

    HashMap<String, FileChannel> stepDrain;
    String fileName;
    String destinationPath;
    Path filePath;

    public VfsWriter() {
        stepDrain = new HashMap<>();
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.fileName = stepExecution.getStepName();
        this.destinationPath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        this.filePath = Paths.get(this.destinationPath + this.fileName);
    }

    @AfterStep
    public void afterStep() {
        try {
            this.stepDrain.get(this.fileName).close();
        } catch (IOException exception) {
            exception.printStackTrace();
        }
    }

    public FileChannel getChannel(String fileName) throws IOException {
        if (this.stepDrain.containsKey(fileName)) {
            return this.stepDrain.get(fileName);
        } else {
            logger.info("creating channel to " + this.filePath.toString());
            prepareFile();
            prepareDirectory();
            FileChannel channel = null;
            try {
                channel = FileChannel.open(this.filePath, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
                stepDrain.put(fileName, channel);
            } catch (IOException exception) {
                exception.printStackTrace();
            }
            return channel;
        }
    }

    public void prepareFile() throws IOException{
        try {
            Files.createFile(this.filePath);
        } catch (FileAlreadyExistsException fileAlreadyExistsException) {
            logger.warn("Tried to create a file when it already existed");
        }
    }

    public void prepareDirectory() {
        Path directories = Paths.get(this.destinationPath);
        if (!Files.exists(directories)) {
            try {
                Files.createDirectories(directories);
            } catch (IOException exception) {
                exception.printStackTrace();
            }
        }
    }

    @Override
    public void write(List<? extends DataChunk> items) throws Exception {
        for(int i = 0; i < items.size(); i++){
            DataChunk chunk = items.get(i);
            logger.info("The current chunk is " + i + " and the current start position is "+ chunk.getStartPosition());
            int bytesWritten = getChannel(chunk.getFileName()).write(ByteBuffer.wrap(chunk.getData()), chunk.getStartPosition());
            if (chunk.getSize() != bytesWritten)
                logger.info("Wrote " + bytesWritten + " but we should have written " + chunk.getSize());
        }
    }
}
