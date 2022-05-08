package org.onedatashare.transferservice.odstransferservice.service.step.ftp;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.SetPool;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.pools.FtpConnectionPool;
import org.onedatashare.transferservice.odstransferservice.service.MetricCache;
import org.onedatashare.transferservice.odstransferservice.service.cron.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.AfterWrite;
import org.springframework.batch.core.annotation.BeforeRead;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.util.List;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;


public class FTPWriter implements ItemWriter<DataChunk>, SetPool {

    Logger logger = LoggerFactory.getLogger(FTPWriter.class);

    String stepName;
    OutputStream outputStream;
    private String dBasePath;
    AccountEndpointCredential destCred;
    FileObject foDest;
    private FtpConnectionPool connectionPool;
    private FTPClient client;
    private StepExecution stepExecution;
    @Setter
    private MetricsCollector metricsCollector;
    @Getter
    @Setter
    private MetricCache metricCache;

    private LocalDateTime readStartTime;

    public FTPWriter(AccountEndpointCredential destCred) {
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
    public void afterStep() {
        logger.info("Inside FTP afterStep");
        try {
            if (outputStream != null) outputStream.close();
        } catch (Exception ex) {
            logger.error("Not able to close the input Stream");
            ex.printStackTrace();
        }
        this.connectionPool.returnObject(this.client);
    }

    public OutputStream getStream(String fileName) {
        if (outputStream == null) {
            try {
                this.outputStream = this.client.storeFileStream(this.dBasePath + "/" + fileName);
            } catch (IOException e) {
                e.printStackTrace();
            }
            logger.info("Stream not present...creating OutputStream for " + fileName);
            //ftpDest();
        }
        return this.outputStream;
    }

    public void ftpDest() {
        logger.info("Creating ftpDest for :" + this.stepName);
        try {
            FileSystemOptions opts = FtpUtility.generateOpts();
            StaticUserAuthenticator auth = new StaticUserAuthenticator(null, this.destCred.getUsername(), this.destCred.getSecret());
            DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);
            String wholeThing;
            if (!dBasePath.endsWith("/")) dBasePath += "/";
            if (this.destCred.getUri().contains("ftp://")) {
                wholeThing = this.destCred.getUri() + "/" + dBasePath + this.stepName;
            } else {
                wholeThing = "ftp://" + this.destCred.getUri() + "/" + dBasePath + this.stepName;
            }
            foDest = VFS.getManager().resolveFile(wholeThing, opts);
            foDest.createFile();
            outputStream = foDest.getContent().getOutputStream();
        } catch (Exception ex) {
            logger.error("Error in setting ftp connection...");
            ex.printStackTrace();
        }
    }

    @BeforeRead
    public void beforeRead() {
        this.readStartTime = LocalDateTime.now();
        logger.info("Before write start time {}", this.readStartTime);
    }


    public void write(List<? extends DataChunk> list) {
        logger.info("Inside Writer---writing chunk of : " + list.get(0).getFileName());
        String fileName = list.get(0).getFileName();
        OutputStream destination = getStream(fileName);
        try {
            for (DataChunk b : list) {
                destination.write(b.getData());
                this.client.setRestartOffset(b.getStartPosition());
            }
        } catch (IOException e) {
            logger.error("Error during writing chunks...exiting");
            e.printStackTrace();
        }
        try {
            destination.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @AfterWrite
    public void afterWrite(List<? extends DataChunk> items) {
        ODSConstants.metricsForOptimizerAndInflux(items, this.readStartTime, logger, stepExecution, metricCache, metricsCollector);
    }


    @Override
    public void setPool(ObjectPool connectionPool) {
        this.connectionPool = (FtpConnectionPool) connectionPool;
    }

}
