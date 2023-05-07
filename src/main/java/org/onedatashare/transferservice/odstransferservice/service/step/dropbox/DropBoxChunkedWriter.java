package org.onedatashare.transferservice.odstransferservice.service.step.dropbox;

import com.box.sdk.BoxUser;
import com.dropbox.core.DbxException;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.CommitInfo;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.UploadSessionCursor;
import com.dropbox.core.v2.users.FullAccount;
import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.AuthenticateCredentials;
import org.onedatashare.transferservice.odstransferservice.service.InfluxCache;
import org.onedatashare.transferservice.odstransferservice.service.cron.MetricsCollector;
import org.onedatashare.transferservice.odstransferservice.service.step.ODSBaseWriter;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;

import java.io.ByteArrayInputStream;
import java.nio.file.Paths;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.OWNER_ID;

public class DropBoxChunkedWriter extends ODSBaseWriter implements ItemWriter<DataChunk> {

    private OAuthEndpointCredential credential;
    private String destinationPath;
    private DbxClientV2 client;
    String sessionId;
    private UploadSessionCursor cursor;
    Logger logger = LoggerFactory.getLogger(DropBoxChunkedWriter.class);
    private String fileName;
    private FileMetadata uploadSessionFinishUploader;

    private AuthenticateCredentials authenticateCredentials;

    String ownerId;
    String usersEmail;


    public DropBoxChunkedWriter(OAuthEndpointCredential credential, MetricsCollector metricsCollector, InfluxCache influxCache, AuthenticateCredentials authenticateCredentials) {
        super(metricsCollector, influxCache);
        this.credential = credential;
        this.authenticateCredentials = authenticateCredentials;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) throws DbxException {
        this.destinationPath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        assert this.destinationPath != null;
        this.client = new DbxClientV2(ODSUtility.dbxRequestConfig, this.credential.getToken());
        sessionId = this.client.files().uploadSessionStart().finish().getSessionId();
        this.stepExecution = stepExecution;
        this.cursor = new UploadSessionCursor(sessionId, 0);
        this.ownerId = stepExecution.getJobParameters().getString(OWNER_ID);
        this.usersEmail = client.users().getCurrentAccount().getEmail();
    }

    @SneakyThrows
    @AfterStep
    public ExitStatus afterStep(StepExecution stepExecution) {
        String path = Paths.get(this.destinationPath, this.fileName).toString();
        path = "/"+path;
        CommitInfo commitInfo = CommitInfo.newBuilder(path).build();
        this.uploadSessionFinishUploader = this.client.files().uploadSessionFinish(cursor, commitInfo).finish();
        return stepExecution.getExitStatus();
    }

    @Override
    public void write(List<? extends DataChunk> items) throws Exception {
        if(this.credential.isTokenExpires()){
            this.credential = authenticateCredentials.checkExpiryAndGenerateNew( usersEmail, ODSConstants.DROPBOX, ownerId);
            this.client = new DbxClientV2(ODSUtility.dbxRequestConfig, this.credential.getToken());
        }
        for(DataChunk chunk : items){
            this.client.files().uploadSessionAppendV2(cursor).uploadAndFinish(new ByteArrayInputStream(chunk.getData()));
            this.cursor = new UploadSessionCursor(sessionId, chunk.getStartPosition() + chunk.getSize());
            logger.info("Current chunk in DropBox Writer " + chunk.toString());
        }
        this.fileName= items.get(0).getFileName();
    }
}
