package org.onedatashare.transferservice.odstransferservice.service.step.box;

import com.box.sdk.*;
import org.onedatashare.transferservice.odstransferservice.model.BoxSmallFileUpload;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;

import java.util.HashMap;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;

public class BoxWriterSmallFile implements ItemWriter<DataChunk> {

    private BoxAPIConnection boxAPIConnection;
    EntityInfo fileInfo;
    private HashMap<String, BoxFileUploadSession> fileMap;
    String destinationBasePath;
    BoxFolder boxFolder;
    BoxSmallFileUpload smallFileUpload;
    private String fileName;
    Logger logger = LoggerFactory.getLogger(BoxWriterSmallFile.class);

    public BoxWriterSmallFile(OAuthEndpointCredential credential, EntityInfo fileInfo){
        this.boxAPIConnection = new BoxAPIConnection(credential.getToken());
        this.fileInfo = fileInfo;
        this.fileMap = new HashMap<>();
        smallFileUpload = new BoxSmallFileUpload();
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.destinationBasePath = stepExecution.getJobParameters().getString(DEST_BASE_PATH); //path to place the files
        this.boxFolder = new BoxFolder(this.boxAPIConnection, this.destinationBasePath);
    }

    /**
     * Executes after we finish making all the write() calls
     * For small file uplaods this is where we actually write all the data
     * For large file uploads we upload the hash we compute and commit so Box constructs the file
     */
    @AfterStep
    public void afterStep() {
        boxFolder.uploadFile(this.smallFileUpload.condenseListToOneStream(this.fileInfo.getSize()), fileName);
    }


    @Override
    public void write(List<? extends DataChunk> items) throws Exception {
        this.fileName = items.get(0).getFileName();
        this.smallFileUpload.addAllChunks(items);
        logger.info("Small file box writer wrote {} DataChunks", items.size());
    }
}
