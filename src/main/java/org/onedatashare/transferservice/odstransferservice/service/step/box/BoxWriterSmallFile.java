package org.onedatashare.transferservice.odstransferservice.service.step.box;
 
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

import java.util.List;

import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.SmallFileUpload;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.InfluxCache;
import org.onedatashare.transferservice.odstransferservice.service.MetricsCollector;
import org.onedatashare.transferservice.odstransferservice.service.step.ODSBaseWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxFolder;

public class BoxWriterSmallFile extends ODSBaseWriter implements ItemWriter<DataChunk> {

    EntityInfo fileInfo;
    String destinationBasePath;
    BoxFolder boxFolder;
    SmallFileUpload smallFileUpload;
    Logger logger = LoggerFactory.getLogger(BoxWriterSmallFile.class);
    private final BoxAPIConnection boxAPIConnection;
    private String fileName;

    public BoxWriterSmallFile(OAuthEndpointCredential credential, EntityInfo fileInfo, MetricsCollector metricsCollector, InfluxCache influxCache) {
        super(metricsCollector, influxCache);
        this.boxAPIConnection = new BoxAPIConnection(credential.getToken());
        this.fileInfo = fileInfo;
        smallFileUpload = new SmallFileUpload();
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.destinationBasePath = stepExecution.getJobParameters().getString(DEST_BASE_PATH); //path to place the files
        this.boxFolder = new BoxFolder(this.boxAPIConnection, this.destinationBasePath);
        this.stepExecution = stepExecution;
    }

    /**
     * Executes after we finish making all the write() calls
     * For small file uplaods this is where we actually write all the data
     * For large file uploads we upload the hash we compute and commit so Box constructs the file
     */
    @AfterStep
    public ExitStatus afterStep(StepExecution stepExecution) {
        boxFolder.uploadFile(this.smallFileUpload.condenseListToOneStream(), fileName);
        return stepExecution.getExitStatus();
    }

    @Override
    public void write(Chunk<? extends DataChunk> chunk) throws Exception {
        List<? extends DataChunk> items = chunk.getItems();
        this.fileName = items.get(0).getFileName();
        this.smallFileUpload.addAllChunks(items);
        logger.info("Small file box writer wrote {} DataChunks", items.size());

    }
}
