package org.onedatashare.transferservice.odstransferservice;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
//@EnableEurekaClient
@EnableScheduling
@EnableBatchProcessing
public class OdsTransferService {

    public static void main(String[] args) {
        SpringApplication.run(OdsTransferService.class, args);
    }

}

