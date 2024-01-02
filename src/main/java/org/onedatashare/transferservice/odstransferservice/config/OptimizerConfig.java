package org.onedatashare.transferservice.odstransferservice.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.DefaultUriBuilderFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class OptimizerConfig {

    @Value("${optimizer.url}")
    private String optimizerUrl;

    @Bean
    public RestTemplate optimizerTemplate() {
        return new RestTemplateBuilder()
                .uriTemplateHandler(new DefaultUriBuilderFactory(optimizerUrl))
                .build();
    }

    @Bean(name ="optimizerTaskExecutor")
    public Executor optimizerTaskExecutor(){
        return Executors.newVirtualThreadPerTaskExecutor();
    }
}
