package org.onedatashare.transferservice.odstransferservice.config;

import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class DataSourceConfig {

//    public DataSource getH2DataSource(){
//        return DataSourceBuilder.create()
//                .driverClassName("org.h2.Driver")
//                .url("jdbc:h2:mem:testdb")
//                .username("sa")
//                .password("")
//                .build();
//    }
//    public DataSource getCockroachDBDataSource(){
//        return DataSourceBuilder.create()
//                .driverClassName("org.postgresql.Driver")
//                .url("postgreql://localhost:25257/defaultdb?sslmode=disable")
//                .username("root")
//                .password("")
//                .build();
//    }
}

