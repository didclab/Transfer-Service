package org.onedatashare.transferservice.odstransferservice.service.DatabaseService;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class JobQueryService {
    @Autowired
    JobExplorer jobExplorer;
    @Autowired
    JobRepository jobRepository;

    public JobExecution getJobExecution(JobInstance jobInstance) {
        return jobExplorer.getLastJobExecution(jobInstance);
    }
    public JobInstance getLastJobInstance(String jobName) {
        return jobExplorer.getLastJobInstance(jobName);
    }

    public StepExecution getLastStepExecution(JobInstance jobInstance, String stepName) {
        return jobRepository.getLastStepExecution(jobInstance,stepName);
    }

}
