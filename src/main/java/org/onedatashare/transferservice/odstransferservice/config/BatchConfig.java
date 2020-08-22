package org.onedatashare.transferservice.odstransferservice.config;

import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.service.TransferService;
import org.onedatashare.transferservice.odstransferservice.service.listner.JobCompletionListener;
import org.onedatashare.transferservice.odstransferservice.service.step.Writer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.separator.RecordSeparatorPolicy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.util.ArrayList;
import java.util.List;


@Configuration
public class BatchConfig {

    private TransferJobRequest transferJobRequest = TransferService.getTransferJobRequest();

    List<Resource> resources = setResource();

    private List<Resource> setResource() {
        List<Resource> temp = new ArrayList<>();
        String basePath = transferJobRequest.getSource().getInfo().getPath();
        for (EntityInfo entityInfo : transferJobRequest.getSource().getInfoList()) {
            temp.add(new FileSystemResource(basePath + entityInfo.getPath()));
        }
        return temp;
    }


    @Bean
    public Job job(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        Step step = stepBuilderFactory.get("sampleSetp")
                .<String, String>chunk(10)
                .reader(stringMultiResourceItemReader())
                .writer(new Writer())
                .build();

        return jobBuilderFactory.get("job")
                .incrementer(new RunIdIncrementer()).listener(listener())
                .start(step)
                .build();
    }

    @Bean
    public MultiResourceItemReader<String> stringMultiResourceItemReader() {

        MultiResourceItemReader<String> resourceItemReader = new MultiResourceItemReader<>();
        resourceItemReader.setResources((Resource[]) resources.toArray());
        resourceItemReader.setDelegate(fileItemReader());
        return resourceItemReader;
    }

    @Bean
    public FlatFileItemReader<String> fileItemReader() {
        FlatFileItemReader<String> reader = new FlatFileItemReader<String>();
        //reader.setResource(new FileSystemResource(transferJobRequest.getSource().getInfo().getPath() + transferJobRequest.getSource().getInfoList().get(0)));
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


        return reader;
    }

    @Bean
    public JobExecutionListener listener() {
        return new JobCompletionListener();
    }

}