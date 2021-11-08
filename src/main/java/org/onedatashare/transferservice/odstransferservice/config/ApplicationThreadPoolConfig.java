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

    @Setter
    @Getter
    private int TRANSFER_POOL_SIZE=32;
    @Setter
    @Getter
    private int JOB_POOL_SIZE=1;
    @Setter
    @Getter
    private int JOB_MAX_POOL_SIZE=1;
    @Setter
    @Getter
    private int STEP_POOL_SIZE=5;
    @Setter
    @Getter
    private int STEP_MAX_POOL_SIZE=20;

    @Getter
    @Setter
    private int parallelThreadPoolSize = 20;

    public ThreadPoolTaskExecutor stepTaskExecutor(){
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(STEP_POOL_SIZE);
        executor.setThreadNamePrefix("step");
        executor.setKeepAliveSeconds(60);
        executor.initialize();
        return executor;
    }

    public TaskExecutor sequentialThreadPool(){
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setThreadNamePrefix("sequential");
        executor.setKeepAliveSeconds(60);
        executor.initialize();
        return executor;
    }

    public TaskExecutor parallelThreadPool(){
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(this.parallelThreadPoolSize);
        executor.setThreadNamePrefix("parallel");
        executor.setKeepAliveSeconds(60);
        executor.initialize();
        return executor;
    }
}
