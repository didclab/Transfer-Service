package org.onedatashare.transferservice.odstransferservice.service;

import org.onedatashare.transferservice.odstransferservice.model.optimizer.OptimizerCreateRequest;
import org.onedatashare.transferservice.odstransferservice.model.optimizer.OptimizerDeleteRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

@Service
public class OptimizerService {

    @Autowired
    RestTemplate optimizerTemplate;

    @Value("${spring.application.name}")
    String appName;

    HttpHeaders headers;

    Logger logger = LoggerFactory.getLogger(OptimizerService.class);

    public OptimizerService() {
        headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
    }

    public void createOptimizerBlocking(OptimizerCreateRequest optimizerCreateRequest) throws RestClientException {
        optimizerCreateRequest.setNodeId(this.appName);
        logger.info("Sending OptimizerCreateRequest {}", optimizerCreateRequest);
        HttpEntity<OptimizerCreateRequest> createRequestHttpEntity = new HttpEntity<>(optimizerCreateRequest, this.headers);
        logger.info(createRequestHttpEntity.getBody().toString());
        this.optimizerTemplate.postForLocation("/optimizer/create", createRequestHttpEntity, Void.class);
    }

    public void deleteOptimizerBlocking(OptimizerDeleteRequest optimizerDeleteRequest) {
        optimizerDeleteRequest.setNodeId(this.appName);
        try {
            this.optimizerTemplate.postForObject("/optimizer/delete", new HttpEntity<>(optimizerDeleteRequest, this.headers), Void.class);
        } catch (RestClientException e) {
            logger.error("Failed to Delete optimizer. {}", optimizerDeleteRequest);
        }
        logger.info("Deleted {}", optimizerDeleteRequest);
    }
}
