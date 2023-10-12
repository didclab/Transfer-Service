package org.onedatashare.transferservice.odstransferservice.config;

import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.HashSet;
import java.util.Set;

@Configuration
public class BatchConfig {

//    @Bean
//    public JobLauncher jobLauncher(JobRepository jobRepository) {
//        TaskExecutorJobLauncher taskExecutorJobLauncher = new TaskExecutorJobLauncher();
//        taskExecutorJobLauncher.setJobRepository(jobRepository);
//        return taskExecutorJobLauncher;
//    }

    @Bean
    public Set<Long> jobIds() {
        return new HashSet<>();
    }

    @Bean
    public PlatformTransactionManager transactionManager() {
        return new JpaTransactionManager();
    }

    @Bean
    public JobLauncher asyncJobLauncher(JobRepository jobRepository) {
        TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
        return jobLauncher;
    }
}

