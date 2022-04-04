//package org.onedatashare.transferservice.odstransferservice.config;
//
//import org.onedatashare.transferservice.odstransferservice.cron.metric.NetworkMetric;
//import org.springframework.boot.autoconfigure.domain.EntityScan;
//import org.springframework.boot.context.properties.ConfigurationProperties;
//import org.springframework.boot.jdbc.DataSourceBuilder;
//import org.springframework.context.annotation.*;
//import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
//import org.springframework.jdbc.datasource.DataSourceTransactionManager;
//import org.springframework.transaction.PlatformTransactionManager;
//
//import javax.sql.DataSource;
//
///**
// * @author deepika
// */
//@Configuration
//@PropertySource({"classpath:application.properties"})
//@EnableJpaRepositories(basePackages= "org.onedatashare.transferservice.odstransferservice.DataRepository"
////        entityManagerFactoryRef = "productEntityManager",
//        )
//public class JobDetailsDataSourceConfig {
//
//    @Primary
//    @Bean
//    @ConfigurationProperties(prefix="spring.datasource")
//    public DataSource jobDetailsDataSource() {
//        return DataSourceBuilder.create().build();
//    }
//
////    @Bean
////    public PlatformTransactionManager transactionManager() {
////        PlatformTransactionManager transactionManager = new DataSourceTransactionManager(jobDetailsDataSource());
////        return transactionManager;
////    }
//
//}
