package org.onedatashare.transferservice.odstransferservice.config;


import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HeaderIterator;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.concurrent.TimeUnit;

@Configuration
@EnableScheduling
@NoArgsConstructor
@Getter
@Setter
public class HttpClientConfig {
    private static final Logger LOG = LoggerFactory.getLogger(HttpClientConfig.class);
    private int connectionTimeOut = 30000;
    private int requestTimeOut = 30000; //30 seconds until we kill
    private int socketTimeOut = 60000; //60 seconds without data and we kill that connection
    private int maxConnections = 20;
    private long defaultKeepAliveTimeMillis = 20000; //20 seconds until the connection dies
    private int closeIdleConnectionWaitTime = 30;
    private int poolSize = 10;

    @Bean
    public PoolingHttpClientConnectionManager poolingHttpClientConnectionManager() {
        PoolingHttpClientConnectionManager poolingHttpClientConnectionManager = new PoolingHttpClientConnectionManager();
        poolingHttpClientConnectionManager.setMaxTotal(maxConnections);
        return poolingHttpClientConnectionManager;
    }

    @Bean
    public CloseableHttpClient httpClient() {
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(getRequestTimeOut())
                .setConnectTimeout(getConnectionTimeOut())
                .setSocketTimeout(getSocketTimeOut()).build();

        return HttpClients.custom()
                .setDefaultRequestConfig(requestConfig)
                .setConnectionManager(poolingHttpClientConnectionManager())
                .build();
    }
    /**
    * This keep alive strategy is to ensure no connection stays open indefinently.
     * Use this if you do not know if the Rest API u r querying has the "Keep-Alive" in the header.
     * This will ensure no Thread hangs on an infinite connection
     **/

    @Bean
    public ConnectionKeepAliveStrategy connectionKeepAliveStrategy() {
        return (httpResponse, httpContext) -> {
            HeaderIterator headerIterator = httpResponse.headerIterator(HTTP.CONN_KEEP_ALIVE);
            HeaderElementIterator elementIterator = new BasicHeaderElementIterator(headerIterator);

            while (elementIterator.hasNext()) {
                HeaderElement element = elementIterator.nextElement();
                String param = element.getName();
                String value = element.getValue();
                if (value != null && param.equalsIgnoreCase("timeout")) {
                    return Long.parseLong(value) * 1000; // convert to ms
                }
            }

            return getDefaultKeepAliveTimeMillis();
        };
    }

    /**
     * This deploys a Thread to check if there any stale/hanging connections in the Pool currently, if so they get closed
     * This runs once every 10 seconds, might need to be more frequent
     * @param manager
     * @return
     */

    @Bean
    public Runnable idleConnectionMonitor(final PoolingHttpClientConnectionManager manager) {
        return new Runnable() {
            @Override
            @Scheduled(fixedDelay = 10000) //run this every 10 seconds
            public void run() {
                try {
                    if (manager != null) {
                        LOG.trace("run IdleConnectionMonitor - Closing expired and idle connections...");
                        manager.closeExpiredConnections();
                        manager.closeIdleConnections(getCloseIdleConnectionWaitTime(), TimeUnit.SECONDS);
                    } else {
                        LOG.trace("run IdleConnectionMonitor - Http Client Connection manager is not initialised");
                    }
                } catch (Exception e) {
                    LOG.error("run IdleConnectionMonitor - Exception occurred. msg={}, e={}", e.getMessage(), e);
                }
            }
        };
    }

    @Bean
    public TaskScheduler idleMonitorPool() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setThreadNamePrefix("idle-connection-monitor-pool");
        scheduler.setPoolSize(3);
        return scheduler;
    }
}