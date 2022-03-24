package org.onedatashare.transferservice.odstransferservice.service.step.dropbox;

import com.dropbox.core.DbxException;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.CommitInfo;
import com.dropbox.core.v2.files.UploadSessionCursor;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;

import java.io.ByteArrayInputStream;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;

public class DropBoxWriter implements ItemWriter<DataChunk> {

    private final OAuthEndpointCredential credential;
    private String destinationPath;
    private DbxClientV2 client;
    String sessionId;
    private UploadSessionCursor cursor;
    Logger logger = LoggerFactory.getLogger(DropBoxWriter.class);

    public DropBoxWriter(OAuthEndpointCredential credential){
        this.credential = credential;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) throws DbxException {
            this.destinationPath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
            assert this.destinationPath != null;
            this.client = new DbxClientV2(ODSUtility.dbxRequestConfig, this.credential.getToken());
            logger.info("session id is + " + this.client.files().uploadSessionStart().finish().getSessionId());
            sessionId = this.client.files().uploadSessionStart().finish().getSessionId();
    }

    @AfterStep
    public void afterStep() throws DbxException {
        CommitInfo commitInfo = CommitInfo.newBuilder(this.destinationPath).build();
        this.client.files().uploadSessionFinish(cursor, commitInfo).finish();
    }

    @Override
    public void write(List<? extends DataChunk> items) throws Exception {
        this.cursor = new UploadSessionCursor(sessionId, items.get(0).getStartPosition());
        for(DataChunk chunk : items){
            this.client.files().uploadSessionAppendV2(cursor).uploadAndFinish(new ByteArrayInputStream(chunk.getData()));
            this.cursor = new UploadSessionCursor(sessionId, chunk.getStartPosition() + chunk.getSize());
            logger.info("Current chunk in DropBox Writer " + chunk.toString());
        }
    }
}