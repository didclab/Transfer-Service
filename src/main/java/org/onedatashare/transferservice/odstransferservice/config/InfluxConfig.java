package org.onedatashare.transferservice.odstransferservice.config;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

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
