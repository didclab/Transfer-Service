package org.onedatashare.transferservice.odstransferservice.service.step.dropbox;

import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.WriteMode;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.SmallFileUpload;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.InfluxCache;
import org.onedatashare.transferservice.odstransferservice.service.MetricsCollector;
import org.onedatashare.transferservice.odstransferservice.service.step.ODSBaseWriter;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

import java.io.InputStream;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;

public class DropBoxWriterSmallFile extends ODSBaseWriter implements ItemWriter<DataChunk> {

    EntityInfo fileInfo;
    String destinationBasePath;
    SmallFileUpload smallFileUpload;
    DbxClientV2 dropboxClient;
    Logger logger = LoggerFactory.getLogger(DropBoxWriterSmallFile.class);
    private String fileName;

    public DropBoxWriterSmallFile(OAuthEndpointCredential credential, EntityInfo fileInfo, MetricsCollector metricsCollector, InfluxCache influxCache) {
        super(metricsCollector, influxCache);
        this.dropboxClient = new DbxClientV2(ODSUtility.dbxRequestConfig, credential.getToken());
        this.fileInfo = fileInfo;
        smallFileUpload = new SmallFileUpload();
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.destinationBasePath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        this.stepExecution = stepExecution;
    }

    @AfterStep
    public ExitStatus afterStep(StepExecution stepExecution) throws Exception {
        try (InputStream inputStream = this.smallFileUpload.condenseListToOneStream()) {
            dropboxClient.files().uploadBuilder(destinationBasePath + "/" + fileName)
                    .withMode(WriteMode.ADD)
                    .uploadAndFinish(inputStream);
        } catch (Exception e) {
            logger.error("Error uploading file to Dropbox: ", e);
            return ExitStatus.FAILED;
        }
        return stepExecution.getExitStatus();
    }


    @Override
    public void write(Chunk<? extends DataChunk> chunk) throws Exception {
        List<? extends DataChunk> items = chunk.getItems();
        this.fileName = items.get(0).getFileName();
        this.smallFileUpload.addAllChunks(items);
        logger.info("Small file Dropbox writer wrote {} DataChunks", items.size());
    }
}