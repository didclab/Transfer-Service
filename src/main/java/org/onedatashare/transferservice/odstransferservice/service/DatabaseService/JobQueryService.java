package org.onedatashare.transferservice.odstransferservice.service.DatabaseService;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class JobQueryService {
    @Autowired
    JobExplorer jobExplorer;

    public JobExecution getJobExecution(JobInstance jobInstance) {
        return jobExplorer.getLastJobExecution(jobInstance);
    }
    public JobInstance getLastJobInstance(String jobName) {
        return jobExplorer.getLastJobInstance(jobName);
    }

    //ToDO
    //Write Query for Steps
    //When Each File will be as a Single Step

}
