package org.onedatashare.transferservice.odstransferservice.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;
import org.onedatashare.transferservice.odstransferservice.service.FileTransferNodeRegistrationLifeCycleListener;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;

import java.util.UUID;

@Configuration
public class HazelcastClientConfig {

    private final Environment environment;
    private final ObjectMapper objectMapper;

    public HazelcastClientConfig(Environment environment, ObjectMapper objectMapper) {
        this.environment = environment;
        this.objectMapper = objectMapper;
    }

    @Value("spring.application.name")
    private String appName;

    @Bean
    @Qualifier("clientConfig")
    @Profile("local")
    public ClientConfig devClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("dev-scheduler-cluster");
        clientConfig.getNetworkConfig().addAddress("127.0.0.1");
        return clientConfig;
    }

    @Bean
    @Qualifier("clientConfig")
    @Profile({"prod", "eks", "ec2",})
    public ClientConfig prodClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("prod-scheduler-cluster");
        clientConfig.getNetworkConfig().getEurekaConfig().setEnabled(true);
        return clientConfig;
    }

    @Bean
    public HazelcastInstance hazelcastInstance(ClientConfig clientConfig) {
        HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);
        FileTransferNodeRegistrationLifeCycleListener fileTransferNodeRegistrationEventListener = new FileTransferNodeRegistrationLifeCycleListener(hazelcastInstance, environment, objectMapper);
        hazelcastInstance.getLifecycleService().addLifecycleListener(fileTransferNodeRegistrationEventListener);
        return hazelcastInstance;
    }

    @Bean
    public IMap<String, HazelcastJsonValue> fileTransferNodeRegistrationMap(@Qualifier("hazelcastInstance") HazelcastInstance hazelcastInstance) {
        return hazelcastInstance.getMap("file-transfer-node-map");
    }

    @Bean
    public IMap<UUID, HazelcastJsonValue> fileTransferScheduleMap(@Qualifier("hazelcastInstance") HazelcastInstance hazelcastInstance) {
        return hazelcastInstance.getMap("file-transfer-schedule-map");
    }

    @Bean
    public IMap<String, HazelcastJsonValue> carbonIntensityMap(@Qualifier("hazelcastInstance") HazelcastInstance hazelcastInstance) {
        return hazelcastInstance.getMap("carbon-intensity-map");
    }

    @Bean
    public IQueue<HazelcastJsonValue> messageQueue(@Qualifier("hazelcastInstance") HazelcastInstance hazelcastInstance) {
        return hazelcastInstance.getQueue(appName);
    }
}
