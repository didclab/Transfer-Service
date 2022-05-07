package org.onedatashare.transferservice.odstransferservice.service;

import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.Metric;
import org.onedatashare.transferservice.odstransferservice.model.optimizer.Optimizer;
import org.onedatashare.transferservice.odstransferservice.model.optimizer.OptimizerInputRequest;
import org.onedatashare.transferservice.odstransferservice.pools.ThreadPoolManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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

    Logger logger = LoggerFactory.getLogger(OptimizerCron.class);

    @Override
    public void run() {
        ConcurrentHashMap<String, Metric> cache = metricCache.threadCache;
        //running active job so we want to push and ask optimizer
        HashMap<String, ThreadPoolTaskExecutor> threadPoolMap = threadPoolManager.getExecutorHashmap();
        int parallelism = 0;
        int concurrency = 0;
        for (String key : threadPoolMap.keySet()) {
            ThreadPoolTaskExecutor pool = threadPoolMap.get(key);
            logger.info("Pool Prefix {} with active thread count {} and pool size of {}", pool.getThreadNamePrefix(), pool.getActiveCount(), pool.getPoolSize());
            if (key.contains(ODSConstants.PARALLEL_POOL_PREFIX)) {
                ThreadPoolTaskExecutor parallelPool = threadPoolMap.get(key);
                parallelism = parallelPool.getActiveCount();
            }
            if (key.contains(ODSConstants.STEP_POOL_PREFIX)) {
                ThreadPoolTaskExecutor concPool = threadPoolMap.get(key);
                concurrency = concPool.getActiveCount();
            }
            if (parallelism > 0 && concurrency > 0) {
                break;
            }
        }
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
            inputRequest.setThroughput(stats.getAverage());
            Optimizer optimizer = this.optimizerService.inputToOptimizerBlocking(inputRequest);
            logger.info("Optimizer gave us {}", optimizer);
            this.threadPoolManager.applyOptimizer(optimizer.getConcurrency(), optimizer.getParallelism());
            for (String key : cache.keySet()) {
                Metric metric = cache.remove(key);
                metric.getStepExecution().setCommitCount(optimizer.getPipelining());
                logger.info("Step {} commitCount {}",metric.getStepExecution(), metric.getStepExecution().getCommitCount());
            }
        }
    }
}
