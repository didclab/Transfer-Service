package org.onedatashare.transferservice.odstransferservice.service.step.googleDrive;

import com.onedatashare.commonservice.model.credential.OAuthEndpointCredential;
import org.apache.commons.pool2.ObjectPool;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.SetPool;
import org.onedatashare.transferservice.odstransferservice.pools.GDriveConnectionPool;
import org.onedatashare.transferservice.odstransferservice.utility.GDriveHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.annotation.BeforeWrite;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.http.HttpStatus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.GOOGLE_DRIVE_MIN_BYTES;


public class GDriveResumableWriter implements ItemWriter<DataChunk>, SetPool {

    Logger logger = LoggerFactory.getLogger(GDriveResumableWriter.class);
    private final OAuthEndpointCredential credential;
    private final EntityInfo fileInfo;
    private String basePath;
    private String fileName;
    private GDriveConnectionPool connectionPool;
    private GDriveHelper utility;

    private DataChunk readyChunk;

    private boolean failed;
    private boolean success;

    public GDriveResumableWriter(OAuthEndpointCredential credential, EntityInfo fileInfo) {
        this.credential = credential;
        this.fileInfo = fileInfo;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) throws IOException, GeneralSecurityException {
        this.basePath = stepExecution.getJobExecution().getJobParameters().getString(ODSConstants.DEST_BASE_PATH);
        this.readyChunk = null;
        this.failed = false;
        this.success = false;
    }

    @BeforeWrite
    public void beforeWrite(List<? extends DataChunk> items) throws IOException, InterruptedException {
        if (this.utility == null || this.utility.getSessionUri() == null) {
            this.logger.debug("Initializing resumable upload");
            this.utility = GDriveHelper.builder()
                    .connectionPool(this.connectionPool)
                    .fileInfo(this.fileInfo)
                    .credential(this.credential)
                    .build();
            this.fileName = items.get(0).getFileName();
            int status = this.utility.initializeUpload(this.fileName, this.basePath);
            if (status != HttpStatus.OK.value()) {
                throw new IOException("Unable to get the Location header from google drive. Error response code: " + status);
            }
        }
    }

    @Override
    public void write(Chunk<? extends DataChunk> chunk) {
        List<? extends DataChunk> dataChunkList = chunk.getItems();
        try {
            int chunkIndex = 0;
            while (chunkIndex < dataChunkList.size()) {
                DataChunk currentChunk = dataChunkList.get(chunkIndex);
                if (readyChunk == null || (readyChunk.getData().length < GOOGLE_DRIVE_MIN_BYTES && this.fileInfo.getSize() - 1 - readyChunk.getStartPosition() > GOOGLE_DRIVE_MIN_BYTES)) {
                    writeToReadyChunk(currentChunk);
                    chunkIndex++;
                    continue;
                }
                logger.info("Currently starting to write: {}", readyChunk);

                HttpResponse<String> response = this.utility.uploadChunk(readyChunk);
                int responseCode = response.statusCode();
                String rangeHeader = response.headers().firstValue("RANGE").get();
                if (responseCode == 200 || responseCode == 201) {
                    success = true;
                    return;
                } else if (responseCode == 308) {
                    if (rangeHeader != null) {
                        rangeHeader = rangeHeader.substring("bytes=".length());
                        String[] ranges = rangeHeader.split("-");
                        Long bytesWritten = Long.valueOf(ranges[1]);
                        updateReadyChunk(bytesWritten);
                    }
                }
            }
        } catch (Exception ex) {
            failed = true;
            logger.error("Failed to write file to the server", ex);

        }
    }

    @AfterStep
    public void writeLastChunk() throws IOException, URISyntaxException, InterruptedException {
        if (success == true) {
            return;
        }
        if (failed == true) {
            throw new IOException("Could not transfer file to the server");
        }
        while (readyChunk != null) {
            HttpResponse<String> response = this.utility.uploadChunk(readyChunk);
            int responseCode = response.statusCode();
            Optional<String> rangeHeader = response.headers().firstValue("RANGE");
            if (responseCode == 200 || responseCode == 201) {
                return;
            } else if (responseCode == 308) {
                if (!rangeHeader.isEmpty()) {
                    String range = rangeHeader.get().substring("bytes=".length());
                    String[] ranges = range.split("-");
                    Long bytesWritten = Long.valueOf(ranges[1]);
                    updateReadyChunk(bytesWritten);
                }
            } else {
                throw new IOException("Could not write the file to the server");

            }
        }
    }

    private void updateReadyChunk(Long bytesWritten) {
        int from = (int) (bytesWritten - readyChunk.getStartPosition() + 1);
        if (from == readyChunk.getData().length) {
            readyChunk = null;
        }
        readyChunk.setData(Arrays.copyOfRange(readyChunk.getData(), from, readyChunk.getData().length));
        readyChunk.setStartPosition(bytesWritten + 1);
    }

    private void writeToReadyChunk(DataChunk currentChunk) throws IOException {
        if (this.readyChunk == null) {
            this.readyChunk = currentChunk;
            return;
        }
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(this.readyChunk.getData());
        outputStream.write(currentChunk.getData());
        byte[] data = outputStream.toByteArray();

        this.readyChunk.setData(data);
    }

    @Override
    public void setPool(ObjectPool connectionPool) {
        this.connectionPool = (GDriveConnectionPool) connectionPool;
    }

}
