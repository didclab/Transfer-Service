package org.onedatashare.transferservice.odstransferservice.config;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.influx.InfluxConfig;
import io.micrometer.influx.InfluxMeterRegistry;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InfluxDBConfig {
    @Value("${management.metrics.export.influx.org}")
    private String influxOrg;

    @Value("${management.metrics.export.influx.db}")
    private String influxBucket;

    @Value("${management.metrics.export.influx.token}")
    private String influxToken;

    @Value("${management.metrics.export.influx.uri}")
    private String influxUri;

    @Bean
    public InfluxMeterRegistry getRegistry() {
        InfluxConfig config = new InfluxConfig() {
            @Override
            public String org() {
                return influxOrg;
            }

            @NotNull
            @Override
            public String bucket() {
                return influxBucket;
            }

            @Override
            public String token() {
                return influxToken;
            }

            @NotNull
            @Override
            public String uri() {
                return influxUri;
            }

            @Override
            public String get(@NotNull String k) {
                return null;
            }
        };
        InfluxMeterRegistry registry = new InfluxMeterRegistry(config, Clock.SYSTEM);
        Metrics.addRegistry(registry);
        return registry;
    }
}
