package org.onedatashare.transferservice.odstransferservice.service.step.vfs;

import com.onedatashare.commonservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.service.InfluxCache;
import org.onedatashare.transferservice.odstransferservice.service.MetricsCollector;
import org.onedatashare.transferservice.odstransferservice.service.step.ODSBaseWriter;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;

public class VfsWriter extends ODSBaseWriter implements ItemWriter<DataChunk> {

    AccountEndpointCredential destCredential;
    FileChannel fileChannel;
    String destinationPath;
    Path filePath;

    public VfsWriter(AccountEndpointCredential credential, EntityInfo fileInfo, MetricsCollector metricsCollector, InfluxCache influxCache) {
        super(metricsCollector, influxCache);
        this.destCredential = credential;
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

    public void prepareDirectories() {
        if (!Files.exists(Paths.get(this.destinationPath))) {
            try {
                Files.createDirectories(Paths.get(this.destinationPath));
            } catch (FileAlreadyExistsException fileAlreadyExistsException) {
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void write(Chunk<? extends DataChunk> chunks) throws Exception {
        List<? extends DataChunk> items = chunks.getItems();
        if (this.fileChannel == null) {
            Path path = Paths.get(this.destinationPath, items.get(0).getFileName());
            this.fileChannel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            this.filePath = path;
        }
        for (int i = 0; i < items.size(); i++) {
            DataChunk chunk = items.get(i);
            int bytesWritten = this.fileChannel.write(ByteBuffer.wrap(chunk.getData()), chunk.getStartPosition());
            chunk = null;
        }
    }
}
