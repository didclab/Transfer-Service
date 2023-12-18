package org.onedatashare.transferservice.odstransferservice;

import org.onedatashare.transferservice.odstransferservice.config.VaultConfiguration;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableDiscoveryClient
@EnableScheduling
public class OdsTransferService {


    public static void main(String[] args) {
        VaultConfiguration.loadSecrets();
        SpringApplication.run(OdsTransferService.class, args);
    }

}

