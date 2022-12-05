package org.onedatashare.transferservice.odstransferservice.service.step.ftp;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.pool2.ObjectPool;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.SetPool;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.pools.FtpConnectionPool;
import org.onedatashare.transferservice.odstransferservice.service.MetricCache;
import org.onedatashare.transferservice.odstransferservice.service.cron.MetricsCollector;
import org.onedatashare.transferservice.odstransferservice.service.step.ODSBaseWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;
import org.springframework.retry.support.RetryTemplate;

import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;


public class FTPWriterODS extends ODSBaseWriter implements ItemWriter<DataChunk>, SetPool {

    Logger logger = LoggerFactory.getLogger(FTPWriterODS.class);

    String stepName;
    OutputStream outputStream;
    private String dBasePath;
    AccountEndpointCredential destCred;
    private FtpConnectionPool connectionPool;
    private FTPClient client;
    private StepExecution stepExecution;
    @Setter
    private MetricsCollector metricsCollector;
    @Getter
    @Setter
    private MetricCache metricCache;

    private LocalDateTime readStartTime;

    private RetryTemplate retryTemplate;

    public FTPWriterODS(AccountEndpointCredential destCred) {
        this.destCred = destCred;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        logger.info("Inside FTP beforeStep");
        outputStream = null;
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
        if(outputStream == null){
            try {
                this.outputStream = this.client.storeFileStream(this.dBasePath+"/"+fileName);
            } catch (IOException ex) {
                logger.error("Error in opening outputstream in FTP Writer for file : {}", fileName );
                throw ex;
            }
            logger.info("Stream not present...creating OutputStream for " + fileName);
            //ftpDest();
        }
        return this.outputStream;
    }

    public void write(List<? extends DataChunk> list) throws IOException {
        logger.info("Inside Writer---writing chunk of : " + list.get(0).getFileName());
        String fileName = list.get(0).getFileName();

        this.retryTemplate.execute((c) -> {
            try {
                if (this.outputStream == null) {
                    this.outputStream = getStream(fileName);
                }
                for (DataChunk b : list) {
                    logger.info("Current chunk in FTP Writer " + b.toString());
                    this.outputStream.write(b.getData());
                    this.client.setRestartOffset(b.getStartPosition());
                }
                this.outputStream.flush();
            } catch (IOException ex) {
                this.outputStream = null;
                this.invalidateAndCreateNewClient();
                throw ex;
            }
            return null;
        });

    }

    @Override
    public void setPool(ObjectPool connectionPool) {
        this.connectionPool = (FtpConnectionPool) connectionPool;
    }

    public void setRetryTemplate(RetryTemplate retryTemplate) {
        this.retryTemplate = retryTemplate;
    }

    private void invalidateAndCreateNewClient() {
        this.connectionPool.invalidateAndCreateNewClient(this.client);
        try {
            this.client = this.connectionPool.borrowObject();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
