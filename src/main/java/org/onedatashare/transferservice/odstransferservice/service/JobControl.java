package org.onedatashare.transferservice.odstransferservice.service;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.controller.TransferController;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.service.step.CustomReader;
import org.onedatashare.transferservice.odstransferservice.service.step.FTPWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.UrlResource;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
@NoArgsConstructor
@Getter
@Setter
public class JobControl {
    int chunckSize; //by default this is the file size
    public TransferJobRequest request;
    Step parent;
    Logger logger = LoggerFactory.getLogger(JobControl.class);



    @Autowired
    FTPWriter ftpWriter;

    @Autowired
    JobBuilderFactory jobBuilderFactory;

    @Autowired
    StepBuilderFactory stepBuilderFactory;

    @SneakyThrows
    private List<Step> createSteps(List<EntityInfo> infoList){
        List<Step> steps = new ArrayList<>();
        for(EntityInfo file : infoList){
            CustomReader customReader = new CustomReader();
            UrlResource urlResource = new UrlResource("ftp://user:pass@localhost:2121/Downloads/outputTransfer/"+ file.getPath());
            customReader.setResource(urlResource);
            SimpleStepBuilder<DataChunk, DataChunk> child = stepBuilderFactory.get(file.getPath()).<DataChunk, DataChunk>chunk(getChunckSize());
            switch (request.getSource().getType()){
                case ftp:
                    child.reader(customReader).writer(ftpWriter).build();
                    break;
            }
            steps.add(child.build());
        logger.warn(urlResource.getFilename());
        }
        return steps;
    }

    @Lazy
    @Bean
    public Job createJobDefinition(){
        List<Step> steps = createSteps(request.getSource().getInfoList());
        SimpleJobBuilder builder = jobBuilderFactory.get(request.getOwnerId())
                .incrementer(new RunIdIncrementer()).start(steps.get(0));
        logger.info(steps.remove(0).getName() + " in Job Control create job def\n");
        for(Step step : steps){
            logger.info(step.getName() + " in Job Control create job def\n");
            builder.next(step);
        }
        return builder.build();
    }
}
