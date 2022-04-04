//package org.onedatashare.transferservice.odstransferservice.config;
//
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.autoconfigure.domain.EntityScan;
//import org.springframework.boot.context.properties.ConfigurationProperties;
//import org.springframework.boot.jdbc.DataSourceBuilder;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.context.annotation.Primary;
//import org.springframework.context.annotation.PropertySource;
//import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
//import org.springframework.jdbc.datasource.DataSourceTransactionManager;
//import org.springframework.transaction.PlatformTransactionManager;
//import org.springframework.transaction.annotation.EnableTransactionManagement;
//
//import javax.sql.DataSource;
//
///**
// * @author deepika
// */
//
//@Configuration
//@EnableTransactionManagement
//@PropertySource({"classpath:application.properties"})
//@EnableJpaRepositories(basePackages= "org.onedatashare.transferservice.odstransferservice.cron.metric",
////        entityManagerFactoryRef = "productEntityManager",
//        transactionManagerRef = "metricsTransactionManager")
//
//@EntityScan(
//        basePackages = { "org.onedatashare.transferservice.odstransferservice.cron.metric" }
////        entityManagerFactoryRef = "redEntityManagerFactory"
//)
//public class MetricsDataSourceConfig {
//
//    private DataSource dataSource;
//    private PlatformTransactionManager transactionManager;
//
//    @Bean
//    @ConfigurationProperties(prefix="spring.second-datasource")
//    public DataSource metricsDataSource() {
//        return DataSourceBuilder.create().build();
//    }
//
//    @Autowired(required = false)
//    public void setDatasource(DataSource datasource) {
//        this.dataSource = datasource;
//        this.transactionManager = new DataSourceTransactionManager(dataSource);
//    }
//
//    @Bean
//    public PlatformTransactionManager metricsTransactionManager() {
//        PlatformTransactionManager transactionManager = new DataSourceTransactionManager(metricsDataSource());
//        return transactionManager;
//    }
//}