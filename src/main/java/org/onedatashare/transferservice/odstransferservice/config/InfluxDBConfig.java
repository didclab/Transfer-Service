package org.onedatashare.transferservice.odstransferservice.config;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InfluxDBConfig {
    @Value("${ods.influx.org}")
    private String influxOrg;

    @Value("${ods.influx.bucket}")
    private String influxBucket;

    @Value("${ods.influx.token}")
    private String influxToken;

    @Value("${ods.influx.uri}")
    private String influxUri;

    @Value("${spring.application.name}")
    private String appName;

    @Bean
    public InfluxDBClient influxClient() {
        InfluxDBClientOptions influxDBClientOptions = InfluxDBClientOptions.builder()
                .url(this.influxUri)
                .org(this.influxOrg)
                .bucket(this.influxBucket)
                .authenticateToken(this.influxToken.toCharArray())
                .addDefaultTag("APP_NAME", this.appName)
                .build();
        return InfluxDBClientFactory.create(influxDBClientOptions);
    }
}
