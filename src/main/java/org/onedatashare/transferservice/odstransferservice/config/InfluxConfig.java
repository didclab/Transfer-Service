package org.onedatashare.transferservice.odstransferservice.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Data
public class InfluxConfig {
    @Value("${influxdb.token}")
    private String token;

    @Value("${influxdb.url}")
    private String url;

    @Value("${influxdb.bucket}")
    private String bucket;

    @Value("${influxdb.org}")
    private String org;
}
