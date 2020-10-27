package org.onedatashare.transferservice.odstransferservice.service;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.config.ApplicationThreadPoolConfig;
import org.onedatashare.transferservice.odstransferservice.config.DataSourceConfig;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
//import org.onedatashare.transferservice.odstransferservice.service.listner.JobCompletionListener;
import org.onedatashare.transferservice.odstransferservice.service.listner.JobCompletionListener;
import org.onedatashare.transferservice.odstransferservice.service.step.ftp.FTPReader;
import org.onedatashare.transferservice.odstransferservice.service.step.ftp.FTPWriter;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;

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
    public JobLauncher asyncJobLauncher() {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(this.createJobRepository());
        jobLauncher.setTaskExecutor(threadPoolConfig.jobRequestThreadPool());
        logger.info("Job launcher for the transfer controller has a thread pool");
        return jobLauncher;
    }

    //    @Bean
    @SneakyThrows
    protected JobRepository createJobRepository() {
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
        factory.setDataSource(dataSource);
        factory.setTransactionManager(transactionManager);
        factory.setIsolationLevelForCreate("ISOLATION_SERIALIZABLE");
        factory.setTablePrefix("BATCH_");
        factory.setMaxVarCharLength(1000);
        return factory.getObject();
    }


    int chunkSize; //by default this is the file size
    public TransferJobRequest request;
    Step parent;
    Logger logger = LoggerFactory.getLogger(JobControl.class);


    @Autowired
    private ApplicationContext context;

    @Autowired
    JobBuilderFactory jobBuilderFactory;

    @Autowired
    StepBuilderFactory stepBuilderFactory;


    private List<Flow> createConcurrentFlow(List<EntityInfo> infoList, String basePath, String id, String pass) throws MalformedURLException {
        logger.info("CreateConcurrentFlow function");
        List<Flow> flows = new ArrayList<>();
        for (EntityInfo file : infoList) {

            FTPReader ftpReader = new FTPReader();
            FTPWriter ftpWriter = new FTPWriter();
            SimpleStepBuilder<DataChunk, DataChunk> child = stepBuilderFactory.get(file.getPath()).<DataChunk, DataChunk>chunk(1024);
            switch (request.getSource().getType()) {
                case ftp:
                    child.reader(ftpReader).writer(ftpWriter)
                            .faultTolerant()
                            .retry(Exception.class)
                            .retryLimit(2)
                            .build();
                    break;
            }
            flows.add(new FlowBuilder<Flow>(id + basePath).start(child.build()).build());

        }
        return flows;
    }


//    @SneakyThrows
//    private List<Step> createSteps(List<EntityInfo> infoList, String basePath, String id, String pass) {
//        List<Step> steps = new ArrayList<>();
//
//        for (EntityInfo file : infoList) {
////            FlowBuilder fb = new FlowBuilder("flow");
//            CustomReader customReader = new CustomReader();
//            FTPWriter ftpWriter = new FTPWriter();
//            String url = basePath.substring(0, 6) + id + ":" + pass + "@" + basePath.substring(6);
////            System.out.println("this is url: "+url);
//            UrlResource urlResource = new UrlResource(url + file.getPath());
//            customReader.setResource(urlResource);
//            SimpleStepBuilder<DataChunk, DataChunk> child = stepBuilderFactory.get(file.getPath()).<DataChunk, DataChunk>chunk(20);
//            switch (request.getSource().getType()) {
//                case ftp:
//                    child.reader(customReader).writer(ftpWriter).build();
//                    break;
//            }
//            steps.add(child.build());
//            logger.warn(urlResource.getFilename());
//        }
//        return steps;
//    }


    @Lazy
    @Bean
    public Job concurrentJobDefination() throws MalformedURLException {
        logger.info("createJobDefination function");
        List<Flow> flows = createConcurrentFlow(request.getSource().getInfoList(),
                request.getSource().getInfo().getPath(), request.getSource().getCredential().getAccountId(),
                request.getSource().getCredential().getPassword());
        Flow[] fl = new Flow[flows.size()];
        Flow f = new FlowBuilder<SimpleFlow>("splitFlow").split(threadPoolConfig.stepTaskExecutor()).add(flows.toArray(fl))
                .build();
        return jobBuilderFactory.get(request.getOwnerId()).listener(new JobCompletionListener())
                .incrementer(new RunIdIncrementer()).start(f).build().build();
    }
}
