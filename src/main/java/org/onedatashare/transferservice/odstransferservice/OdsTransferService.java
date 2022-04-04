package org.onedatashare.transferservice.odstransferservice;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
//@EnableEurekaClient
@EnableBatchProcessing
public class OdsTransferService {

    public static void main(String[] args) {
        SpringApplication.run(OdsTransferService.class, args);
    }

}

