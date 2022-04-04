package org.onedatashare.transferservice.odstransferservice.config;

import javax.sql.DataSource;

import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DataSourceConfig {

    public DataSource getH2DataSource(){
        return DataSourceBuilder.create()
                .driverClassName("org.h2.Driver")
                .url("jdbc:h2:mem:testdb")
                .username("sa")
                .password("")
                .build();
    }
    public DataSource getCockroachDBDataSource(){
        return DataSourceBuilder.create()
                .driverClassName("org.postgresql.Driver")
                .url("postgreql://localhost:25257/defaultdb?sslmode=disable")
                .username("root")
                .password("")
                .build();
    }
}

