package org.onedatashare.transferservice.odstransferservice.config;

import lombok.SneakyThrows;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Profile("hsql")
@Configuration
public class HsqlConfiguration {

    @Primary
    @Bean
    @SneakyThrows
    public JobRepository jobRepository(DataSource dataSource, PlatformTransactionManager transactionManager) {
        JobRepositoryFactoryBean fb = new JobRepositoryFactoryBean();
        fb.setDatabaseType("HSQL");
        fb.setDataSource(dataSource);
        fb.setTransactionManager(transactionManager);
        return fb.getObject();
    }
}
