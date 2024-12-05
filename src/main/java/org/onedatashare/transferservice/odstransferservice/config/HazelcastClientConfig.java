package org.onedatashare.transferservice.odstransferservice.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.collection.IQueue;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;
import org.onedatashare.transferservice.odstransferservice.service.VaultSSLService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.vault.core.VaultTemplate;

import java.util.Properties;
import java.util.Set;
import java.util.UUID;

@Configuration
public class HazelcastClientConfig {

    private final Environment env;
    private final ObjectMapper objectMapper;
    private final VaultSSLService vaultSslService;

    public HazelcastClientConfig(Environment environment, ObjectMapper objectMapper, VaultTemplate vaultTemplate, VaultSSLService vaultSSLService) {
        this.env = environment;
        this.objectMapper = objectMapper;
        this.vaultSslService = vaultSSLService;
    }

    @Value("${spring.application.name}")
    private String appName;

    @Bean
    @Qualifier("clientConfig")
    @Profile("local")
    public ClientConfig devClientConfig(SSLConfig sslConfig) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("dev-scheduler-cluster");
        clientConfig.getNetworkConfig().setSSLConfig(sslConfig);
        clientConfig.setInstanceName(this.appName);
        return clientConfig;
    }

    @Bean
    @Qualifier("clientConfig")
    @Profile({"prod", "eks", "ec2",})
    public ClientConfig prodClientConfig(SSLConfig sslConfig) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("prod-scheduler-cluster");
        clientConfig.getNetworkConfig().setSSLConfig(sslConfig);
        clientConfig.getNetworkConfig().addAddress(env.getProperty("hz.ipaddr", "localhost"));
        return clientConfig;
    }

    @Bean
    public SSLConfig sslConfig() {
        Properties properties = new Properties();
        properties.setProperty("protocol", "TLSv1.2");
        properties.setProperty("mutualAuthentication", "OPTIONAL");
        properties.setProperty("trustStore", this.vaultSslService.getStorePath().toAbsolutePath().toString());
        properties.setProperty("trustStorePassword", env.getProperty("hz.keystore.password", "changeit"));
        properties.setProperty("trustStoreType", "PKCS12");
        properties.setProperty("keyMaterialDuration", this.vaultSslService.getStoreDuration().toString());
        properties.setProperty("validateIdentity", "false");

        SSLConfig sslConfig = new SSLConfig();
        sslConfig.setEnabled(true);
        sslConfig.setProperties(properties);
        return sslConfig;
    }

    @Bean
    public HazelcastInstance hazelcastInstance(ClientConfig clientConfig) {
        clientConfig.addLabel(this.env.getProperty("spring.application.name"));
        return HazelcastClient.newHazelcastClient(clientConfig);
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
    public IMap<UUID, HazelcastJsonValue> carbonIntensityMap(@Qualifier("hazelcastInstance") HazelcastInstance hazelcastInstance) {
        return hazelcastInstance.getMap("carbon-intensity-map");
    }

    @Bean
    public IQueue<HazelcastJsonValue> messageQueue(@Qualifier("hazelcastInstance") HazelcastInstance hazelcastInstance) {
        return hazelcastInstance.getQueue(appName);
    }
}
