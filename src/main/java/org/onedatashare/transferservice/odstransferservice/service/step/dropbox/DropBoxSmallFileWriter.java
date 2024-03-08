package org.onedatashare.transferservice.odstransferservice.service.step.dropbox;

import com.dropbox.core.DbxException;
import com.dropbox.core.v2.DbxClientV2;
import org.onedatashare.transferservice.odstransferservice.model.BoxSmallFileUpload;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.InfluxCache;
import org.onedatashare.transferservice.odstransferservice.service.MetricsCollector;
import org.onedatashare.transferservice.odstransferservice.service.step.ODSBaseWriter;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

import java.io.IOException;
import java.nio.file.Paths;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;

public class DropBoxSmallFileWriter extends ODSBaseWriter implements ItemWriter<DataChunk> {

    private final OAuthEndpointCredential credential;
    private final DbxClientV2 client;
    private final EntityInfo fileInfo;
    private final BoxSmallFileUpload smallFileUpload;
    private String destinationBasePath;

    public DropBoxSmallFileWriter(OAuthEndpointCredential credential, EntityInfo fileInfo, MetricsCollector metricsCollector, InfluxCache influxCache) {
        super(metricsCollector, influxCache);
        this.credential = credential;
        this.client = new DbxClientV2(ODSUtility.dbxRequestConfig, this.credential.getToken());
        this.smallFileUpload = new BoxSmallFileUpload();
        this.fileInfo = fileInfo;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.destinationBasePath = stepExecution.getJobParameters().getString(DEST_BASE_PATH); //path to place the files
        this.stepExecution = stepExecution;
    }

    /**
     * Executes after we finish making all the write() calls
     * For small file uplaods this is where we actually write all the data
     * For large file uploads we upload the hash we compute and commit so Box constructs the file
     */
    @AfterStep
    public ExitStatus afterStep(StepExecution stepExecution) {
        Paths.get(this.destinationBasePath, this.fileInfo.getId());
        try {
            this.client.files().uploadBuilder(this.destinationBasePath)
                    .uploadAndFinish(this.smallFileUpload.condenseListToOneStream());
        } catch (DbxException | IOException e) {
            throw new RuntimeException(e);
        }
        return stepExecution.getExitStatus();
    }

    @Override
    public void write(Chunk<? extends DataChunk> chunk) throws Exception {
        this.smallFileUpload.addAllChunks(chunk.getItems());
    }

}
