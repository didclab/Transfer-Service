package org.onedatashare.transferservice.odstransferservice.service;

import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.Metric;
import org.onedatashare.transferservice.odstransferservice.model.optimizer.Optimizer;
import org.onedatashare.transferservice.odstransferservice.model.optimizer.OptimizerInputRequest;
import org.onedatashare.transferservice.odstransferservice.pools.ThreadPoolManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class OptimizerCron implements Runnable {

    @Autowired
    MetricCache metricCache;

    @Autowired
    OptimizerService optimizerService;

    @Autowired
    ThreadPoolManager threadPoolManager;

    @Value("${optimizer.enable}")
    boolean enableOptimizer;

    Logger logger = LoggerFactory.getLogger(OptimizerCron.class);

    @Autowired
    ConnectionBag connectionBag;

    @Override
    public void run() {
        if (!enableOptimizer) return;
        ConcurrentHashMap<String, Metric> cache = metricCache.threadCache;
        //running active job so we want to push and ask optimizer
        HashMap<String, ThreadPoolTaskExecutor> threadPoolMap = threadPoolManager.getExecutorHashmap();
        int parallelism = threadPoolManager.parallelismCount();
        int concurrency = threadPoolManager.concurrencyCount();
        if (metricCache.threadCache.size() > 0) {
            DoubleSummaryStatistics stats = new DoubleSummaryStatistics();
            OptimizerInputRequest inputRequest = new OptimizerInputRequest();
            inputRequest.setParallelism(parallelism);
            inputRequest.setConcurrency(concurrency);
            for (String key : cache.keySet()) {
                Metric metric = cache.get(key);
                inputRequest.setPipelining(metric.getPipelining());
                stats.accept(metric.getThroughput());
                break;
            }
            inputRequest.setChunkSize(0);
            inputRequest.setThroughput(stats.getAverage());
            Optimizer optimizer = this.optimizerService.inputToOptimizerBlocking(inputRequest);
            logger.info("Optimizer gave us {}", optimizer);
            cache.clear();
            this.threadPoolManager.applyOptimizer(optimizer.getConcurrency(), optimizer.getParallelism());
            //we need to add pipelining to protocols that support it.
        }
    }
}
