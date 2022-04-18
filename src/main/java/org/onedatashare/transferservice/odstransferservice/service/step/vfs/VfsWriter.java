package org.onedatashare.transferservice.odstransferservice.service.step.vfs;

import edu.emory.mathcs.backport.java.util.Arrays;
import org.apache.commons.codec.binary.Hex;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.FileHashValidator;
import org.onedatashare.transferservice.odstransferservice.service.SetFileHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;

public class VfsWriter implements ItemWriter<DataChunk>, SetFileHash {
    Logger logger = LoggerFactory.getLogger(VfsWriter.class);
    AccountEndpointCredential destCredential;
    HashMap<String, FileChannel> stepDrain;
    String fileName;
    String destinationPath;
    Path filePath;
    FileHashValidator fileHashValidator;

    public VfsWriter(AccountEndpointCredential credential) {
        stepDrain = new HashMap<>();
        this.destCredential = credential;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) throws NoSuchAlgorithmException {
        this.destinationPath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        assert this.destinationPath != null;
        this.filePath = Paths.get(this.destinationPath);
        prepareFile();
        fileHashValidator.setWriterMessageDigest(MessageDigest.getInstance(fileHashValidator.getAlgorithm()));

    }

    @AfterStep
    public void afterStep() throws NoSuchAlgorithmException {
        try {
            if(this.stepDrain.containsKey(this.fileName)){
                this.stepDrain.get(this.fileName).close();
            }
        } catch (IOException exception) {
            exception.printStackTrace();
        }
        if(fileHashValidator.isVerify()) {
            if (!fileHashValidator.check()) {
                logger.info("There's a mismatch"); //todo - retry
            } else {
                logger.info("Check sum matches"); //todo - remove log
            }
        }

    }

    public FileChannel getChannel(String fileName) throws IOException {
        if (this.stepDrain.containsKey(fileName)) {
            return this.stepDrain.get(fileName);
        } else {
            logger.info("creating file : " + fileName);
            FileChannel channel = null;
            try {
                channel = FileChannel.open(this.filePath, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
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
            Files.createDirectories(this.filePath);
        }catch (FileAlreadyExistsException fileAlreadyExistsException){
            logger.warn("Already have the file with this path \t" + this.filePath.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * calculating hash after writing all chunks
     */
    @Override
    public void write(List<? extends DataChunk> items) throws Exception {
        this.fileName = items.get(0).getFileName();
        this.filePath = Paths.get(this.filePath.toString(), this.fileName);
        for (int i = 0; i < items.size(); i++) {
            DataChunk chunk = items.get(i);
            FileChannel channel = getChannel(chunk.getFileName());
            int bytesWritten = channel.write(ByteBuffer.wrap(chunk.getData()), chunk.getStartPosition());
            logger.info("Wrote the amount of bytes: " + String.valueOf(bytesWritten));
            if (chunk.getSize() != bytesWritten)
                logger.info("Wrote " + bytesWritten + " but we should have written " + chunk.getSize());

            if(fileHashValidator.isVerify()) {
                fileHashValidator.getWriterMessageDigest().update(chunk.getData());
            }
        }
        if(fileHashValidator.isVerify()){
            String fileHash = new String(Hex.encodeHex(fileHashValidator.getWriterMessageDigest().digest()));
            logger.info("vfs writer hash: "+ fileHash);
            fileHashValidator.setWriterHash(fileHash);
        }
    }

    @Override
    public void setFileHashValidator(FileHashValidator fileHash) {
        fileHashValidator = fileHash;
    }
}
