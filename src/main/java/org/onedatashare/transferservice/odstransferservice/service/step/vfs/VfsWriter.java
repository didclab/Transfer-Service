package org.onedatashare.transferservice.odstransferservice.service.step.vfs;

import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.InfluxCache;
import org.onedatashare.transferservice.odstransferservice.service.cron.MetricsCollector;
import org.onedatashare.transferservice.odstransferservice.service.step.ODSBaseWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;

public class VfsWriter extends ODSBaseWriter implements ItemWriter<DataChunk> {

    Logger logger = LoggerFactory.getLogger(VfsWriter.class);
    AccountEndpointCredential destCredential;
    FileChannel fileChannel;
    String destinationPath;
    Path filePath;
    private final EntityInfo fileInfo;


    public VfsWriter(AccountEndpointCredential credential, EntityInfo fileInfo, MetricsCollector metricsCollector, InfluxCache influxCache) {
        super(metricsCollector, influxCache);
        this.destCredential = credential;
        this.fileInfo = fileInfo;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) throws IOException {
        this.destinationPath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        assert this.destinationPath != null;
        this.filePath = Paths.get(this.destinationPath);
        this.stepExecution = stepExecution;
        prepareDirectories();
    }

    @AfterStep
    public ExitStatus afterStep(StepExecution stepExecution) throws IOException {
        this.fileChannel.close();
        return stepExecution.getExitStatus();
    }

    public void prepareDirectories() throws IOException{
        try {
            Files.createDirectories(Paths.get(this.destinationPath));
        } catch (FileAlreadyExistsException fileAlreadyExistsException) {
            logger.warn("Already have the file with this path \t" + this.filePath.toString());
        }
    }

    @Override
    public void write(List<? extends DataChunk> items) throws Exception {
        Path path = Paths.get(this.destinationPath, items.get(0).getFileName());
        if(!path.toString().equals(this.filePath.toString())){
            this.fileChannel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            this.filePath = path;
        }
        logger.info("VfsWriter file Path: {}", this.filePath);
        for (int i = 0; i < items.size(); i++) {
            DataChunk chunk = items.get(i);
            int bytesWritten = this.fileChannel.write(ByteBuffer.wrap(chunk.getData()), chunk.getStartPosition());
            if (chunk.getSize() != bytesWritten)
                logger.info("Wrote " + bytesWritten + " but we should have written " + chunk.getSize());
            chunk = null;
        }
    }
}
