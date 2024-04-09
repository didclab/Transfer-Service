package org.onedatashare.transferservice.odstransferservice.service;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.pools.ThreadPoolContract;
import org.onedatashare.transferservice.odstransferservice.service.DatabaseService.InfluxIOService;
import org.onedatashare.transferservice.odstransferservice.service.listner.JobCompletionListener;
import org.onedatashare.transferservice.odstransferservice.service.step.ReaderWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;
import java.util.stream.Collectors;


@Service
@NoArgsConstructor
@Getter
@Setter
public class JobControl {

    Logger logger = LoggerFactory.getLogger(JobControl.class);

    @Autowired
    ReaderWriterFactory readerWriterFactory;

    @Autowired
    JobRepository jobRepository;

    @Autowired
    ConnectionBag connectionBag;

    @Autowired
    JobCompletionListener jobCompletionListener;

    @Autowired
    PlatformTransactionManager platformTransactionManager;

    @Autowired
    InfluxIOService influxIOService;

    @Autowired
    ThreadPoolContract threadPool;

    @Autowired
    BackOffPolicy backOffPolicy;

    private List<Flow> createConcurrentFlow(TransferJobRequest request) {
        String basePath = request.getSource().getFileSourcePath();
        return request.getSource().getInfoList().stream().map(file -> {
            String idForStep = "";
            if (!file.getId().isEmpty()) {
                idForStep = file.getId();
            } else {
                idForStep = file.getPath();
            }
            SimpleStepBuilder<DataChunk, DataChunk> stepBuilder = new StepBuilder(idForStep, this.jobRepository)
                    .chunk(request.getOptions().getPipeSize(), this.platformTransactionManager);
            stepBuilder
                    .reader(readerWriterFactory.getRightReader(request.getSource(), file, request.getOptions()))
                    .writer(readerWriterFactory.getRightWriter(request.getDestination(), file));
            if (request.getOptions().getParallelThreadCount() > 0) {
                stepBuilder.taskExecutor(threadPool.parallelPool(request.getOptions().getParallelThreadCount(), file.getPath()));
            }
            stepBuilder.throttleLimit(64);
            stepBuilder.faultTolerant()
                    .backOffPolicy(this.backOffPolicy);
            return new FlowBuilder<Flow>(basePath + idForStep)
                    .start(stepBuilder.build()).build();
        }).collect(Collectors.toList());
    }

    public Job concurrentJobDefinition(TransferJobRequest request) {
        JobBuilder jobBuilder = new JobBuilder(request.getJobUuid().toString(), this.jobRepository);
        connectionBag.preparePools(request);
        List<Flow> flows = createConcurrentFlow(request);
        this.influxIOService.reconfigureBucketForNewJob(request.getOwnerId());
        Flow[] fl = new Flow[flows.size()];
        Flow f = new FlowBuilder<Flow>("splitFlow")
                .split(this.threadPool.stepPool(request.getOptions().getConcurrencyThreadCount()))
                .add(flows.toArray(fl))
                .build();
        return jobBuilder
                .listener(jobCompletionListener)
                .start(f)
                .end()
                .build();
    }

}