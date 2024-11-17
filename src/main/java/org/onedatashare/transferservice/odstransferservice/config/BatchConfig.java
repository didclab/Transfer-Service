package org.onedatashare.transferservice.odstransferservice.config;

import org.onedatashare.transferservice.odstransferservice.model.TimeBoundRetryPolicy;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Configuration
public class BatchConfig {

    @Bean
    public PlatformTransactionManager transactionManager(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean
    public RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();

        TimeBoundRetryPolicy timeBoundRetryPolicy = new TimeBoundRetryPolicy(Duration.ofDays(1).toMillis());
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();

        backOffPolicy.setInitialInterval(500); // Set initial interval
        backOffPolicy.setMultiplier(2.0); // Exponential multiplier
        backOffPolicy.setMaxInterval(5000); // Max backoff interval

        retryTemplate.setBackOffPolicy(backOffPolicy);
        retryTemplate.setRetryPolicy(timeBoundRetryPolicy);
        return retryTemplate;
    }

    @Bean
    public JobLauncher jobLauncher(JobRepository jobRepository) {
        TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
        jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
        jobLauncher.setJobRepository(jobRepository);
        return jobLauncher;
    }


    @Bean
    public BackOffPolicy backOffPolicy() {
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(TimeUnit.SECONDS.toMillis(5));
        backOffPolicy.setMultiplier(2.0);
        backOffPolicy.setMaxInterval(TimeUnit.DAYS.toMillis(1));
        return backOffPolicy;
    }

}

