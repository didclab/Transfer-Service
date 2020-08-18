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


    private final int MAX_POOL_SIZE = 64;

    @Setter
    @Getter
    private int POOL_SIZE=32;

    @Bean
    @Lazy
    @Scope("prototype")
    public TaskExecutor transferTaskExecutor(){
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(POOL_SIZE);
        executor.setMaxPoolSize(MAX_POOL_SIZE);
        return executor;
    }

}
