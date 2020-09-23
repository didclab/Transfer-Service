package org.onedatashare.transferservice.odstransferservice.config;

import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.service.listner.DataBaseOperationStepExecutionListener;
import org.onedatashare.transferservice.odstransferservice.service.listner.JobCompletionListener;
import org.onedatashare.transferservice.odstransferservice.service.step.Processor;
import org.onedatashare.transferservice.odstransferservice.service.step.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;


@Configuration
public class BatchConfig extends DefaultBatchConfigurer {
    public static final Logger LOGGER = LoggerFactory.getLogger(BatchConfig.class);
    private DataSource dataSource;
    private PlatformTransactionManager transactionManager;

    @Autowired
    private ApplicationThreadPoolConfig threadPoolConfig;

    @Autowired
    FlatFileItemReader flatFileItemReader;

    @Autowired
    Writer writer;

    @Autowired
    Processor processor;

    @Autowired
    TaskExecutor stepTaskExecutor;

    @Autowired(required = false)
    public void setDatasource(DataSource datasource){
        this.dataSource = datasource;
        this.transactionManager = new DataSourceTransactionManager(dataSource);

    }

    @Override
    public PlatformTransactionManager getTransactionManager() {
        return transactionManager;
    }

    @Bean
    public JobLauncher asyncJobLauncher() {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(createJobRepository());
        jobLauncher.setTaskExecutor(threadPoolConfig.jobRequestThreadPool());
        LOGGER.info("Job launcher for the transfer controller has a thread pool");
        return jobLauncher;
    }

    @Bean
    public StepExecutionListener crudListener() {
        return new DataBaseOperationStepExecutionListener();
    }

    @Bean
    @SneakyThrows
    protected JobRepository createJobRepository(){
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
        factory.setDataSource(dataSource);
        factory.setTransactionManager(transactionManager);
        factory.setIsolationLevelForCreate("ISOLATION_SERIALIZABLE");
        factory.setTablePrefix("BATCH_");
        factory.setMaxVarCharLength(1000);
        return factory.getObject();
    }

    @Bean
    public Job job(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        Step step = stepBuilderFactory.get("SampleStep")
                .listener(crudListener())
                .<byte[], byte[]>chunk(2)
                .reader(flatFileItemReader)
                .writer(writer)
                .taskExecutor(stepTaskExecutor)
                .build();
        return jobBuilderFactory.get("job").listener(listener())
                .incrementer(new RunIdIncrementer())
                .start(step)
                .build();
    }

    @Bean
    public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry){
        JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor = new JobRegistryBeanPostProcessor();
        jobRegistryBeanPostProcessor.setJobRegistry(jobRegistry);
        return jobRegistryBeanPostProcessor;
    }

    @Bean
    public JobExecutionListener listener() {
        return new JobCompletionListener();
    }

}