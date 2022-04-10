package org.onedatashare.transferservice.odstransferservice.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Data
public class CommandLineOptions {

    @Value("${cmdLine.user}")
    private String user;

    @Value("${cmdLine.interface}")
    private String networkInterface;

    @Value("${cmdLine.length}")
    private String length;

    @Value("${cmdLine.options}")
    private String options;

}
