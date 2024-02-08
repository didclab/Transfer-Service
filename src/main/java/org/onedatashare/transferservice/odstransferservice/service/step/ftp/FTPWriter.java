package org.onedatashare.transferservice.odstransferservice.service.step.ftp;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.pool2.ObjectPool;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.SetPool;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.pools.FtpConnectionPool;
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

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;


public class FTPWriter extends ODSBaseWriter implements ItemWriter<DataChunk>, SetPool {

    private final EntityInfo fileInfo;
    Logger logger = LoggerFactory.getLogger(FTPWriter.class);

    String stepName;
    OutputStream outputStream;
    private String dBasePath;
    AccountEndpointCredential destCred;
    private FtpConnectionPool connectionPool;
    private FTPClient client;

    public FTPWriter(AccountEndpointCredential destCred, EntityInfo fileInfo, MetricsCollector metricsCollector, InfluxCache influxCache) {
        super(metricsCollector, influxCache);
        this.destCred = destCred;
        this.fileInfo = fileInfo;
        this.outputStream = null;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        logger.info("Inside FTP beforeStep");
        dBasePath = stepExecution.getJobParameters().getString(DEST_BASE_PATH);
        stepName = stepExecution.getStepName();
        try {
            this.client = this.connectionPool.borrowObject();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.stepExecution = stepExecution;
    }

    @AfterStep
    public ExitStatus afterStep(StepExecution stepExecution) {
        logger.info("Inside FTP afterStep");
        try {
            if (outputStream != null) outputStream.close();
        } catch (Exception ex) {
            logger.error("Not able to close the input Stream");
            ex.printStackTrace();
        }
        this.connectionPool.returnObject(this.client);
        return stepExecution.getExitStatus();
    }

    private OutputStream getStream(String fileName) throws IOException {
        return this.client.storeFileStream(this.dBasePath + "/" + fileName);
    }


    public void write(List<? extends DataChunk> list) throws IOException {
    }

    @Override
    public void setPool(ObjectPool connectionPool) {
        this.connectionPool = (FtpConnectionPool) connectionPool;
    }


    private void invalidateAndCreateNewClient() {
        this.connectionPool.invalidateAndCreateNewClient(this.client);
        try {
            this.client = this.connectionPool.borrowObject();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Chunk<? extends DataChunk> chunk) {
        List<? extends DataChunk> items = chunk.getItems();
        String fileName = items.get(0).getFileName();

        try {
            if (this.outputStream == null) {
                this.outputStream = getStream(fileName);
            }
            for (DataChunk b : items) {
                logger.info("Current chunk in FTP Writer " + b.toString());
                this.outputStream.write(b.getData());
                this.client.setRestartOffset(b.getStartPosition());
            }
            this.outputStream.flush();
        } catch (IOException ex) {
            this.outputStream = null;
            this.invalidateAndCreateNewClient();
        }

    }
}
