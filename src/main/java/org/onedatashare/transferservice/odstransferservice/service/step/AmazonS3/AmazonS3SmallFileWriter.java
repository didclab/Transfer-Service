package org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import lombok.Setter;
import org.onedatashare.transferservice.odstransferservice.model.AWSSinglePutRequestMetaData;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.pools.S3ConnectionPool;
import org.onedatashare.transferservice.odstransferservice.service.InfluxCache;
import org.onedatashare.transferservice.odstransferservice.service.cron.MetricsCollector;
import org.onedatashare.transferservice.odstransferservice.service.step.ODSBaseWriter;
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

public class AmazonS3SmallFileWriter extends ODSBaseWriter implements ItemWriter<DataChunk> {

    private String fileName;
    private final EntityInfo fileInfo;
    private final AccountEndpointCredential destCredential;
    Logger logger = LoggerFactory.getLogger(AmazonS3SmallFileWriter.class);
    private String destBasepath;
    private AWSSinglePutRequestMetaData putObjectRequest;
    private AmazonS3 client;
    @Setter
    private S3ConnectionPool pool;
    private String bucketName;


    public AmazonS3SmallFileWriter(AccountEndpointCredential destCredential, EntityInfo fileInfo, MetricsCollector metricsCollector, InfluxCache influxCache) {
        super(metricsCollector, influxCache);
        this.fileName = fileInfo.getId();
        this.fileInfo = fileInfo;
        this.destCredential = destCredential;
        this.putObjectRequest = new AWSSinglePutRequestMetaData();
        String[] temp = this.destCredential.getUri().split(":::");
        this.bucketName = temp[1];
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) throws InterruptedException {
        logger.info("Before Step of AmazonS3SmallFileWriter and the step name is {} with file {}", stepExecution.getStepName(), this.fileInfo);
        this.destBasepath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        this.stepExecution = stepExecution;
        this.client = this.pool.borrowObject();
    }


    @AfterStep
    public ExitStatus afterStep(StepExecution stepExecution) {
        PutObjectRequest putObjectRequest = new PutObjectRequest(this.bucketName, Paths.get(this.destBasepath, fileName).toString(), this.putObjectRequest.condenseListToOneStream(this.fileInfo.getSize()), makeMetaDataForSinglePutRequest(this.fileInfo.getSize()));
        PutObjectResult result = client.putObject(putObjectRequest);
        logger.info("Pushed the final chunk of the small file");
        logger.info(result.toString());
        this.putObjectRequest.clear();
        this.pool.returnObject(this.client);
        return stepExecution.getExitStatus();
    }

    public ObjectMetadata makeMetaDataForSinglePutRequest(long size) {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(size);
        return objectMetadata;
    }

    @Override
    public void write(Chunk<? extends DataChunk> chunk) throws Exception {
        List<? extends DataChunk> items = chunk.getItems();
        this.fileName = items.get(0).getFileName();
        this.putObjectRequest.addAllChunks(items);
    }
}
