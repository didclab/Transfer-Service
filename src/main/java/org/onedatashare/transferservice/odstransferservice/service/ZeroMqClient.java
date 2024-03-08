package org.onedatashare.transferservice.odstransferservice.service;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.SneakyThrows;
import org.onedatashare.transferservice.odstransferservice.Enum.MessageType;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.model.optimizer.TransferApplicationParams;
import org.onedatashare.transferservice.odstransferservice.pools.ThreadPoolContract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.UUID;

@Service
public class ZeroMqClient {

    private final ObjectMapper objectMapper;
    private final ThreadPoolContract threadPool;
    private final ZContext zContext;
    Logger logger = LoggerFactory.getLogger(ZeroMqClient.class);

    JobControl jc;

    JobLauncher jobLauncher;

    JobParamService jobParamService;
    ZMQ.Socket routerSocket;

    @Value("${zeromq.ip}")
    private String zeroMqIp;

    @Value("${spring.application.name}")
    String appName;

    UUID transferNodeUuid;
    private Thread.Builder.OfVirtual rmqThread;


    public ZeroMqClient(JobParamService jobParamService, JobLauncher asyncJobLauncher, JobControl jc, ThreadPoolContract threadPool, ZContext zContext) {
        this.jobParamService = jobParamService;
        this.jobLauncher = asyncJobLauncher;
        this.jc = jc;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.objectMapper.setDefaultPropertyInclusion(JsonInclude.Include.ALWAYS);
        this.threadPool = threadPool;
        this.zContext = zContext;
        this.transferNodeUuid = UUID.randomUUID();
    }

    @PostConstruct
    public void start() {
        this.register();
    }

    @PreDestroy
    public void preDestroy() {
        this.deRegister();
    }

    public void register() {
        this.routerSocket = this.zContext.createSocket(SocketType.DEALER);
        String name = this.appName + "-" + this.transferNodeUuid.toString();
        this.routerSocket.setIdentity(name.getBytes());
        this.routerSocket.connect(this.zeroMqIp);
        this.routerSocket.sendMore("".getBytes());
        this.routerSocket.send("REGISTER".getBytes(), 0);
        this.rmqThread = Thread.ofVirtual();
        this.rmqThread.start(this::consumeMessage);
    }

    public void deRegister() {
        String destroy = "DEREGISTER";
        this.routerSocket.sendMore("");
        this.routerSocket.send(destroy.getBytes(), 0);
        this.routerSocket.close();
    }


    @SneakyThrows
    public void consumeMessage() {
        while (!Thread.currentThread().isInterrupted()) {
            String identity = this.routerSocket.recvStr();
            String message = this.routerSocket.recvStr();
            logger.info("Message received: {}", message);
            JsonNode jsonNode = this.objectMapper.readTree(message);
            String messageTypeStr = jsonNode.get("type").asText();
            MessageType type = MessageType.valueOf(messageTypeStr.toUpperCase());
            switch (type) {
                case FILE_TRANSFER_REQUEST:
                    TransferJobRequest request = objectMapper.readValue(message, TransferJobRequest.class);
                    logger.info("Job Received: {}", request.toString());
                    JobParameters parameters = jobParamService.translate(new JobParametersBuilder(), request);
                    jc.setRequest(request);
                    jobLauncher.run(jc.concurrentJobDefinition(), parameters);
                    break;
                case OPTIMIZATION_PARAM_CHANGE_REQUEST:
                    TransferApplicationParams params = objectMapper.readValue(message, TransferApplicationParams.class);
                    logger.info("Parsed TransferApplicationParams: {}", params);
                    this.threadPool.applyOptimizer(params.getConcurrency(), params.getParallelism());
                    break;
            }
        }
    }
}