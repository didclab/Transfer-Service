package org.onedatashare.transferservice.odstransferservice.config;

import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.service.TransferService;
import org.onedatashare.transferservice.odstransferservice.service.listner.JobCompletionListener;
import org.onedatashare.transferservice.odstransferservice.service.step.Writer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.RecordSeparatorPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;


@Configuration
public class BatchConfig {

//    @Autowired
//    private ApplicationThreadPoolConfig threadPoolConfig;
//
//    @Autowired
//    DataSourceConfig datasource;
//
//
//    @Bean
//    public JobLauncher asyncJobLauncher() {
//        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
//        jobLauncher.setJobRepository(createJobRepository());
//        jobLauncher.setTaskExecutor(threadPoolConfig.jobThreadPool());
//        return jobLauncher;
//    }
//
//    @Bean
//    @SneakyThrows
//    protected JobRepository createJobRepository(){
//        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
//        factory.setDataSource(datasource.getH2DataSource());
//        factory.setIsolationLevelForCreate("ISOLATION_SERIALIZABLE");
//        factory.setTablePrefix("BATCH_");
//        factory.setMaxVarCharLength(1000);
//        return factory.getObject();
//    }

//    @StepScope
//    @Value("#{jobParameters['listToTransfer']}")
//    public String list;

    @Bean
    public Job job(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        Step step = stepBuilderFactory.get("sampleSetp")
                .<String, String>chunk(10)
                .reader(multiFileItemReader(null))
                .writer(new Writer())
                .build();

        return jobBuilderFactory.get("job")
                .incrementer(new RunIdIncrementer()).listener(listener())
                .start(step)
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
    public JobExecutionListener listener() {
        return new JobCompletionListener();
    }

}