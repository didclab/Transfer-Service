package org.onedatashare.transferservice.odstransferservice.message;

import com.hazelcast.core.HazelcastJsonValue;
import org.onedatashare.transferservice.odstransferservice.service.JobControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.launch.JobExecutionNotRunningException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class StopJobRequestHandler implements MessageHandler{

    private final JobControl jobControl;
    private final JobOperator jobOperator;
    private Logger logger;

    public StopJobRequestHandler(JobControl jobControl, JobOperator jobOperator) {
        this.jobControl = jobControl;
        this.jobOperator = jobOperator;
        this.logger = LoggerFactory.getLogger(StopJobRequestHandler.class);
    }

    @Override
    public void messageHandler(HazelcastJsonValue jsonMsg) throws IOException {
        JobExecution jobExecution = this.jobControl.getLatestJobExecution();
        try {
            jobOperator.stop(jobExecution.getJobId());
        } catch (NoSuchJobExecutionException | JobExecutionNotRunningException e) {
            logger.error("Error latest job {}", jobExecution);
        }
    }
}
