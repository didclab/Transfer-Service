package org.onedatashare.transferservice.odstransferservice.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastJsonValue;
import org.onedatashare.transferservice.odstransferservice.model.StopJobRequest;
import org.onedatashare.transferservice.odstransferservice.service.JobControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobExecutionNotRunningException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Set;

@Service
public class StopJobRequestHandler implements MessageHandler {

    private final ObjectMapper objectMapper;
    private final JobOperator jobOperator;
    private final JobExplorer jobExplorer;
    private final Logger logger;

    public StopJobRequestHandler(JobExplorer jobExplorer, JobOperator jobOperator, ObjectMapper objectMapper) {
        this.jobOperator = jobOperator;
        this.objectMapper = objectMapper;
        this.jobExplorer = jobExplorer;
        this.logger = LoggerFactory.getLogger(StopJobRequestHandler.class);
    }

    @Override
    public void messageHandler(HazelcastJsonValue jsonMsg) throws IOException {
        StopJobRequest stopJobRequest = this.objectMapper.readValue(jsonMsg.getValue(), StopJobRequest.class);
        Set<JobExecution> jobExecutionSet = this.jobExplorer.findRunningJobExecutions(stopJobRequest.getJobUuid().toString());
        for (JobExecution jobExecution : jobExecutionSet) {
            try {
                jobOperator.stop(jobExecution.getId());
            } catch (NoSuchJobExecutionException | JobExecutionNotRunningException e) {
                logger.error("Was unable to stop job: {} with error message: {}", jobExecution, e.getMessage());
            }
        }
    }
}
