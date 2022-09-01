package org.onedatashare.transferservice.odstransferservice.service.step.dropbox;

import com.dropbox.core.DbxException;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.CommitInfo;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.UploadSessionCursor;
import lombok.Getter;
import lombok.Setter;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.MetricCache;
import org.onedatashare.transferservice.odstransferservice.service.cron.MetricsCollector;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.AfterWrite;
import org.springframework.batch.core.annotation.BeforeRead;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;

import java.io.ByteArrayInputStream;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;

public class DropBoxChunkedWriter implements ItemWriter<DataChunk> {

    private final OAuthEndpointCredential credential;
    private String destinationPath;
    private DbxClientV2 client;
    String sessionId;
    private UploadSessionCursor cursor;
    Logger logger = LoggerFactory.getLogger(DropBoxChunkedWriter.class);
    private StepExecution stepExecution;
    @Setter
    private MetricsCollector metricsCollector;
    @Getter
    @Setter
    private MetricCache metricCache;

    private LocalDateTime readStartTime;
    private String fileName;
    private FileMetadata uploadSessionFinishUploader;


    public DropBoxChunkedWriter(OAuthEndpointCredential credential) {
        this.credential = credential;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) throws DbxException {
        this.destinationPath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        assert this.destinationPath != null;
        this.client = new DbxClientV2(ODSUtility.dbxRequestConfig, this.credential.getToken());
        sessionId = this.client.files().uploadSessionStart().finish().getSessionId();
        this.stepExecution = stepExecution;
        this.cursor = new UploadSessionCursor(sessionId, 0);

    }

    @AfterStep
    public void afterStep() throws DbxException {
        String path = Paths.get(this.destinationPath, this.fileName).toString();
        path = "/"+path;
        CommitInfo commitInfo = CommitInfo.newBuilder(path).build();
        this.uploadSessionFinishUploader = this.client.files().uploadSessionFinish(cursor, commitInfo).finish();
    }

    @Override
    public void write(List<? extends DataChunk> items) throws Exception {
        for(DataChunk chunk : items){
            this.client.files().uploadSessionAppendV2(cursor).uploadAndFinish(new ByteArrayInputStream(chunk.getData()));
            this.cursor = new UploadSessionCursor(sessionId, chunk.getStartPosition() + chunk.getSize());
            logger.info("Current chunk in DropBox Writer " + chunk.toString());
        }
        this.fileName= items.get(0).getFileName();
    }

    @BeforeRead
    public void beforeRead() {
        this.readStartTime = LocalDateTime.now();
        logger.info("Before write start time {}", this.readStartTime);
    }

    @AfterWrite
    public void afterWrite(List<? extends DataChunk> items) {
        ODSConstants.metricsForOptimizerAndInflux(items, this.readStartTime, logger, stepExecution, metricCache, metricsCollector);
    }

}
