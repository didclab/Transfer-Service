package org.onedatashare.transferservice.odstransferservice.service.step.vfs;

import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
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
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;

public class VfsWriter extends ODSBaseWriter implements ItemWriter<DataChunk> {

    Logger logger = LoggerFactory.getLogger(VfsWriter.class);
    AccountEndpointCredential destCredential;
    String fileName;
    FileChannel fileChannel;
    String destinationPath;
    Path filePath;
    private final EntityInfo fileInfo;


    public VfsWriter(AccountEndpointCredential credential, EntityInfo fileInfo) {
        this.destCredential = credential;
        this.fileInfo = fileInfo;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) throws IOException {
        this.destinationPath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        assert this.destinationPath != null;
        this.filePath = Paths.get(this.destinationPath, this.fileInfo.getId());
        this.stepExecution = stepExecution;
        prepareFile();
        this.fileChannel = FileChannel.open(this.filePath, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    }

    @AfterStep
    public ExitStatus afterStep(StepExecution stepExecution) throws IOException {
        this.fileChannel.close();
        return stepExecution.getExitStatus();
    }

    public void prepareFile() {
        try {
            Files.createDirectories(Paths.get(this.destinationPath));
        } catch (FileAlreadyExistsException fileAlreadyExistsException) {
            logger.warn("Already have the file with this path \t" + this.filePath.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(List<? extends DataChunk> items) throws Exception {
        this.fileName = items.get(0).getFileName();
        this.filePath = Paths.get(this.filePath.toString(), this.fileName);
        for (DataChunk chunk : items) {
            FileChannel channel = getChannel(chunk.getFileName());
            int bytesWritten = channel.write(ByteBuffer.wrap(chunk.getData()), chunk.getStartPosition());
            logger.info("Wrote the amount of bytes: " + bytesWritten);
            if (chunk.getSize() != bytesWritten)
                logger.info("Wrote " + bytesWritten + " but we should have written " + chunk.getSize());
            chunk = null;
        }
    }
}
