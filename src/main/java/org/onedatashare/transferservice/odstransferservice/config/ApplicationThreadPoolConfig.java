package org.onedatashare.transferservice.odstransferservice.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;


@Configuration
public class ApplicationThreadPoolConfig{

    private final int TRANSFER_MAX_POOL_SIZE = 64;

    @Setter
    @Getter
    private int TRANSFER_POOL_SIZE=32;
    @Setter
    @Getter
    private int JOB_POOL_SIZE=6;
    @Setter
    @Getter
    private int JOB_MAX_POOL_SIZE=12;
    @Setter
    @Getter
    private int STEP_POOL_SIZE=3;
    @Setter
    @Getter
    private int STEP_MAX_POOL_SIZE=15;

    @Bean(name = "transferTaskExecutor")
    @Lazy
    public TaskExecutor transferTaskExecutor(){
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(TRANSFER_POOL_SIZE);
        executor.setMaxPoolSize(TRANSFER_MAX_POOL_SIZE);
        return executor;
    }

    @Bean
    @Lazy
    public TaskExecutor stepTaskExecutor(){
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(STEP_POOL_SIZE);
        executor.setMaxPoolSize(STEP_MAX_POOL_SIZE);
        return executor;
    }

    @Bean
    @Lazy
    public TaskExecutor jobRequestThreadPool(){
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(JOB_POOL_SIZE);
        executor.setMaxPoolSize(JOB_MAX_POOL_SIZE);
        return executor;
    }
    @Bean
    @Lazy
    public TaskExecutor sequentialThreadPool(){
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        return executor;
    }
}
