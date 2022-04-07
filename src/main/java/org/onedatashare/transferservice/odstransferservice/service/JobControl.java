package org.onedatashare.transferservice.odstransferservice.service;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.TWENTY_MB;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.config.ApplicationThreadPoolConfig;
import org.onedatashare.transferservice.odstransferservice.config.DataSourceConfig;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.service.listner.JobCompletionListener;
import org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3.AmazonS3Reader;
import org.onedatashare.transferservice.odstransferservice.service.step.AmazonS3.AmazonS3Writer;
import org.onedatashare.transferservice.odstransferservice.service.step.box.BoxReader;
import org.onedatashare.transferservice.odstransferservice.service.step.box.BoxWriterLargeFile;
import org.onedatashare.transferservice.odstransferservice.service.step.box.BoxWriterSmallFile;
import org.onedatashare.transferservice.odstransferservice.service.step.dropbox.DropBoxReader;
import org.onedatashare.transferservice.odstransferservice.service.step.dropbox.DropBoxWriter;
import org.onedatashare.transferservice.odstransferservice.service.step.ftp.FTPReader;
import org.onedatashare.transferservice.odstransferservice.service.step.ftp.FTPWriter;
import org.onedatashare.transferservice.odstransferservice.service.step.http.HttpReader;
import org.onedatashare.transferservice.odstransferservice.service.step.scp.SCPReader;
import org.onedatashare.transferservice.odstransferservice.service.step.scp.SCPWriter;
import org.onedatashare.transferservice.odstransferservice.service.step.sftp.SFTPReader;
import org.onedatashare.transferservice.odstransferservice.service.step.sftp.SFTPWriter;
import org.onedatashare.transferservice.odstransferservice.service.step.vfs.VfsReader;
import org.onedatashare.transferservice.odstransferservice.service.step.vfs.VfsWriter;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Optional.ofNullable;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.TWENTY_MB;


@Service
@NoArgsConstructor
@Getter
@Setter
public class JobControl extends DefaultBatchConfigurer {

    private DataSource dataSource;
    private PlatformTransactionManager transactionManager;

    @Autowired
    private ApplicationThreadPoolConfig threadPoolConfig;

    @Autowired
    DataSourceConfig datasource;

    public TransferJobRequest request;
    Step parent;
    Logger logger = LoggerFactory.getLogger(JobControl.class);

    // @Autowired
    // private ApplicationContext context;

    @Autowired
    JobBuilderFactory jobBuilderFactory;

    @Autowired
    StepBuilderFactory stepBuilderFactory;

    @Autowired
    ConnectionBag connectionBag;

    @Autowired
    JobCompletionListener jobCompletionListener;

    @Autowired
    RetryTemplate retryTemplateForReaderAndWriter;

    @Autowired(required = false)
    public void setDatasource(DataSource datasource) {
        this.dataSource = datasource;
        this.transactionManager = new DataSourceTransactionManager(dataSource);
    }

    @Override
    public PlatformTransactionManager getTransactionManager() {
        return transactionManager;
    }

    @Lazy
    @Bean
    public JobLauncher asyncJobLauncher() throws Exception {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(this.createJobRepository());
        jobLauncher.setTaskExecutor(this.threadPoolConfig.sequentialThreadPool());
        logger.info("Job launcher for the transfer controller has a thread pool");
        return jobLauncher;
    }

    //    @Bean
    @SneakyThrows
    protected JobRepository createJobRepository() throws Exception {
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
        factory.setDataSource(dataSource);
        factory.setTransactionManager(transactionManager);
        factory.setIsolationLevelForCreate("ISOLATION_SERIALIZABLE");
        factory.setTablePrefix("BATCH_");
        factory.setMaxVarCharLength(1000);
        return factory.getObject();
    }

    private List<Flow> createConcurrentFlow(List<EntityInfo> infoList, String basePath, String id) {
        logger.info("CreateConcurrentFlow function");
        List<Flow> flows = new ArrayList<>();
        for (EntityInfo file : infoList) {
            String idForStep = "";
            if(!file.getId().isEmpty()){
                idForStep = file.getId();
            }else{
                idForStep = file.getPath();
            }
            SimpleStepBuilder<DataChunk, DataChunk> child = stepBuilderFactory.get(idForStep).<DataChunk, DataChunk>chunk(this.request.getOptions().getPipeSize());
            if(ODSUtility.fullyOptimizableProtocols.contains(this.request.getSource().getType()) && ODSUtility.fullyOptimizableProtocols.contains(this.request.getDestination().getType()) && this.request.getOptions().getParallelThreadCount() > 1){
                threadPoolConfig.setParallelThreadPoolSize(request.getOptions().getParallelThreadCount());
                child.taskExecutor(this.threadPoolConfig.parallelThreadPool());
            }
            child.reader(getRightReader(request.getSource().getType(), file)).writer(getRightWriter(request.getDestination().getType(), file));
            flows.add(new FlowBuilder<Flow>(id + basePath).start(child.build()).build());
        }
        return flows;
    }


    protected AbstractItemCountingItemStreamItemReader<DataChunk> getRightReader(EndpointType type, EntityInfo fileInfo) {
        switch (type) {
            case http:
                HttpReader hr = new HttpReader(fileInfo, request.getSource().getVfsSourceCredential());
                hr.setPool(connectionBag.getHttpReaderPool());
                return hr;
            case vfs:
                return new VfsReader(request.getSource().getVfsSourceCredential(), fileInfo);
            case sftp:
                SFTPReader sftpReader =new SFTPReader(request.getSource().getVfsSourceCredential(), fileInfo, request.getOptions().getPipeSize());
                sftpReader.setPool(connectionBag.getSftpReaderPool());
                sftpReader.setRetryTemplate(retryTemplateForReaderAndWriter);
                return sftpReader;
            case ftp:
                FTPReader ftpReader = new FTPReader(request.getSource().getVfsSourceCredential(), fileInfo);
                ftpReader.setPool(connectionBag.getFtpReaderPool());
                return ftpReader;
            case s3:
                return new AmazonS3Reader(request.getSource().getVfsSourceCredential(), fileInfo);
            case box:
                BoxReader boxReader = new BoxReader(request.getSource().getOauthSourceCredential(), fileInfo);
                boxReader.setMaxRetry(ofNullable(this.request.getOptions().getRetry()).orElse(1));
                return boxReader;
            case dropbox:
                return new DropBoxReader(request.getSource().getOauthSourceCredential(), fileInfo);
            case scp:
                SCPReader reader = new SCPReader(fileInfo);
                reader.setPool(connectionBag.getSftpReaderPool());
                return reader;
        }
        return null;
    }

    protected ItemWriter<DataChunk> getRightWriter(EndpointType type, EntityInfo fileInfo) {
        switch (type) {
            case vfs:
                return new VfsWriter(request.getDestination().getVfsDestCredential());
            case sftp:
                SFTPWriter sftpWriter = new SFTPWriter(request.getDestination().getVfsDestCredential(), request.getOptions().getPipeSize());
                sftpWriter.setPool(connectionBag.getSftpWriterPool());
                sftpWriter.setRetryTemplate(retryTemplateForReaderAndWriter);
                return sftpWriter;
            case ftp:
                FTPWriter ftpWriter = new FTPWriter(request.getDestination().getVfsDestCredential());
                ftpWriter.setPool(connectionBag.getFtpWriterPool());
                ftpWriter.setRetryTemplate(retryTemplateForReaderAndWriter);
                return ftpWriter;
            case s3:
                return new AmazonS3Writer(request.getDestination().getVfsDestCredential(), fileInfo);
            case box:
                if(fileInfo.getSize() < TWENTY_MB){
                    return new BoxWriterSmallFile(request.getDestination().getOauthDestCredential(), fileInfo);
                }else{
                    return new BoxWriterLargeFile(request.getDestination().getOauthDestCredential(), fileInfo);
                }
            case dropbox:
                return new DropBoxWriter(request.getDestination().getOauthDestCredential());
            case scp:
                SCPWriter scpWriter = new SCPWriter(fileInfo);
                scpWriter.setPool(connectionBag.getSftpWriterPool());
                return scpWriter;
        }
        return null;
    }

    public Job concurrentJobDefinition() throws MalformedURLException {
        connectionBag.preparePools(this.request);
        setRetryPolicy();
        List<Flow> flows = createConcurrentFlow(request.getSource().getInfoList(), request.getSource().getParentInfo().getPath(), request.getJobId());
        Flow[] fl = new Flow[flows.size()];
        threadPoolConfig.setSTEP_POOL_SIZE(this.request.getOptions().getConcurrencyThreadCount());
        Flow f = new FlowBuilder<SimpleFlow>("splitFlow").split(this.threadPoolConfig.stepTaskExecutor()).add(flows.toArray(fl))
                .build();
        return jobBuilderFactory.get(request.getOwnerId()).listener(jobCompletionListener)
                .incrementer(new RunIdIncrementer()).start(f).build().build();
    }

    private void setRetryPolicy() {
        Map<Class<? extends Throwable>, Boolean> retryFor = new HashMap<>();
        retryFor.put(IOException.class, true);
        //add other exceptions to retry for in the map above.
        int retryAttempts = ofNullable(this.request.getOptions().getRetry()).orElse(1);
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(retryAttempts, retryFor);
        this.retryTemplateForReaderAndWriter.setRetryPolicy(retryPolicy);
    }
}