package org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.onedatashare.transferservice.odstransferservice.model.AWSMultiPartMetaData;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.pools.S3ConnectionPool;
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

import java.nio.file.Paths;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;


public class AmazonS3LargeFileWriter extends ODSBaseWriter implements ItemWriter<DataChunk> {
    private final String bucketName;
    Logger logger = LoggerFactory.getLogger(AmazonS3LargeFileWriter.class);
    private AWSMultiPartMetaData metaData;
    EntityInfo fileInfo;
    private AmazonS3 client;
    private String destBasepath;
    private boolean firstPass;
    private String uploadId;
    private String fileName;
    private S3ConnectionPool pool;

    //The ClientConfiguration options are the tcp options we can tune.
    public AmazonS3LargeFileWriter(AccountEndpointCredential destCredential, EntityInfo fileInfo, MetricsCollector metricsCollector, InfluxCache influxCache) {
        super(metricsCollector, influxCache);
        this.fileInfo = fileInfo;
        this.firstPass = false;
        String[] temp = destCredential.getUri().split(":::");
        this.bucketName = temp[1];
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) throws InterruptedException {
        logger.info("The S3LargeFileWriter has EntityInfo: " + this.fileInfo.toString());
        this.destBasepath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        this.stepExecution = stepExecution;
        this.client = this.pool.borrowObject();
    }

    public synchronized void prepareS3Transfer(String fileName) {
        if (!this.firstPass) {
            this.metaData = new AWSMultiPartMetaData();
            String key = Paths.get(this.destBasepath, fileName).toString();
            this.metaData.prepareMetaData(client, this.bucketName, key);
            this.uploadId = this.metaData.getInitiateMultipartUploadResult().getUploadId();
            this.firstPass = true;
            this.fileName = key;
        }
    }


    @AfterStep
    public ExitStatus afterStep(StepExecution stepExecution) {
        this.metaData.completeMultipartUpload(client);
        this.metaData.reset();
        this.pool.returnObject(this.client);
        return stepExecution.getExitStatus();
    }

    public void setPool(S3ConnectionPool s3WriterPool) {
        this.pool = s3WriterPool;
    }

    @Override
    public void write(Chunk<? extends DataChunk> chunk) throws Exception {
        List<? extends DataChunk> items = chunk.getItems();
        if (!this.firstPass) {
            prepareS3Transfer(items.get(0).getFileName());
        }
        for (DataChunk currentChunk : items) {
            logger.info(currentChunk.toString());
            UploadPartRequest uploadPartRequest;
            if (currentChunk.getStartPosition() + currentChunk.getSize() == this.fileInfo.getSize()) {
                uploadPartRequest = ODSUtility.makePartRequest(currentChunk, this.bucketName, this.metaData.getInitiateMultipartUploadResult().getUploadId(), this.fileName, true);
            } else {
                uploadPartRequest = ODSUtility.makePartRequest(currentChunk, this.bucketName, this.uploadId, this.fileName, false);
            }
            UploadPartResult uploadPartResult = client.uploadPart(uploadPartRequest);
            this.metaData.addUploadPart(uploadPartResult);
        }

    }
}