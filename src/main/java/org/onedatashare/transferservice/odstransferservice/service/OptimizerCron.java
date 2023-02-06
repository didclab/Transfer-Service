package org.onedatashare.transferservice.odstransferservice.service;

import org.onedatashare.transferservice.odstransferservice.model.optimizer.Optimizer;
import org.onedatashare.transferservice.odstransferservice.pools.ThreadPoolManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;

@Service
public class OptimizerCron implements Runnable {

    private final OptimizerService optimizerService;

    private final ThreadPoolManager threadPoolManager;

    @Value("${optimizer.enable}")
    boolean enableOptimizer;

    Logger logger = LoggerFactory.getLogger(OptimizerCron.class);

    public OptimizerCron(ThreadPoolManager threadPoolManager, OptimizerService optimizerService) {
        this.optimizerService = optimizerService;
        this.threadPoolManager = threadPoolManager;
    }

    @Override
    public void run() {
        logger.info("Optimizer Cron running");
        try {
            Optimizer optimizer = this.optimizerService.getNextApplicationTupleToUse();
            logger.info("Optimizer gave us {}", optimizer);
            this.threadPoolManager.applyOptimizer(optimizer.getConcurrency(), optimizer.getParallelism());
        } catch (RestClientException e) {
            logger.error("Failed to get new parameters from the optimizer, continuing with current parameters");
        }
    }
}
