package org.onedatashare.transferservice.odstransferservice.service.step.vfs;

import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
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
    AccountEndpointCredential destCredential;
    HashMap<String, FileChannel> stepDrain;
    String fileName;
    String destinationPath;
    Path filePath;

    public VfsWriter(AccountEndpointCredential credential) {
        stepDrain = new HashMap<>();
        this.destCredential = credential;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.fileName = stepExecution.getStepName();
        this.destinationPath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        this.filePath = Paths.get(this.destinationPath + this.fileName);
        prepareFile();
    }

    @AfterStep
    public void afterStep() {
        try {
            if(this.stepDrain.containsKey(this.fileName)){
                this.stepDrain.get(this.fileName).close();
            }
        } catch (IOException exception) {
            exception.printStackTrace();
        }
    }

    public FileChannel getChannel(String fileName) throws IOException {
        if (this.stepDrain.containsKey(fileName)) {
            return this.stepDrain.get(fileName);
        } else {
            logger.info("creating file : " + fileName);
            FileChannel channel = null;
            try {
                channel = FileChannel.open(this.filePath, StandardOpenOption.WRITE);
                stepDrain.put(fileName, channel);
            } catch (IOException exception) {
                logger.error("Not Able to open the channel");
                exception.printStackTrace();
            }
            return channel;
        }
    }

    public void prepareFile() {
        try {
            Files.createDirectories(this.filePath.getParent());
            Files.createFile(this.filePath);
        }catch (FileAlreadyExistsException fileAlreadyExistsException){
            logger.error("Already have the file with this path \t" + this.filePath.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(List<? extends DataChunk> items) throws Exception {
        logger.info(Thread.currentThread().getName());
        for (int i = 0; i < items.size(); i++) {
            DataChunk chunk = items.get(i);
            FileChannel channel = getChannel(chunk.getFileName());
            int bytesWritten = channel.write(ByteBuffer.wrap(chunk.getData()), chunk.getStartPosition());
            if (chunk.getSize() != bytesWritten)
                logger.info("Wrote " + bytesWritten + " but we should have written " + chunk.getSize());
        }
    }
}
