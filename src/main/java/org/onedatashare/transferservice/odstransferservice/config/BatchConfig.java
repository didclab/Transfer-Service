package org.onedatashare.transferservice.odstransferservice.config;

import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.service.listner.DataBaseOperationStepExecutionListener;
import org.onedatashare.transferservice.odstransferservice.service.listner.JobCompletionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.separator.RecordSeparatorPolicy;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import java.util.ArrayList;
import java.util.List;


@Configuration
public class BatchConfig {
    public static final Logger LOGGER = LoggerFactory.getLogger(BatchConfig.class);

    @Autowired
    private ApplicationThreadPoolConfig threadPoolConfig;


    @Autowired
    DataSourceConfig datasource;

    @Bean
    public JobLauncher asyncJobLauncher() {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(createJobRepository());
        jobLauncher.setTaskExecutor(threadPoolConfig.jobRequestThreadPool());
        LOGGER.info("Job launcher for the transfer controller has a thread pool");
        return jobLauncher;
    }

    @Bean
    @SneakyThrows
    protected JobRepository createJobRepository(){
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
        factory.setDataSource(datasource.getH2DataSource());
        factory.setTransactionManager(new DataSourceTransactionManager(datasource.getH2DataSource()));
        factory.setIsolationLevelForCreate("ISOLATION_SERIALIZABLE");
        factory.setTablePrefix("BATCH_");
        factory.setMaxVarCharLength(1000);
        return factory.getObject();
    }

    @Bean
    public StepExecutionListener crudListener() {
        return new DataBaseOperationStepExecutionListener();
    }

    @Bean
    public Job job(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        Step step = stepBuilderFactory.get("sampleStep").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                // To Access Job Parameters
                //       String item = chunkContext.getStepContext().getJobParameters().get("item").toString();
                System.out.println("The Step has been started");
                return RepeatStatus.FINISHED;
            }
        }).listener(crudListener()).build();

        Step newStep = stepBuilderFactory.get("sampleStep").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                // To Access Job Parameters
                //       String item = chunkContext.getStepContext().getJobParameters().get("item").toString();
                System.out.println("The new Step has been started");
                return RepeatStatus.FINISHED;
            }
        }).build();

        return jobBuilderFactory.get("newSampleJob15").start(step)
                .next(newStep)
                .build();
    }


    @StepScope
    @Bean
    public MultiResourceItemReader multiFileItemReader(@Value("#{jobParameters['listToTransfer']}") String list) {
        MultiResourceItemReader<String> resourceItemReader = new MultiResourceItemReader<>();
        FlatFileItemReader<String> reader = new FlatFileItemReader<String>();
        List<Resource> temp = new ArrayList<>();
        for (String l : list.split("<::>")) {
            temp.add(new FileSystemResource(l));
        }

        resourceItemReader.setResources((Resource[]) temp.toArray());
        resourceItemReader.setDelegate(reader);
        reader.setRecordSeparatorPolicy(new RecordSeparatorPolicy() {
            @Override
            public boolean isEndOfRecord(String s) {
                if (s.length() == 10)
                    return true;
                return false;
            }

            @Override
            public String postProcess(String s) {
                return s;
            }

            @Override
            public String preProcess(String s) {
                return s;
            }
        });


        return resourceItemReader;
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