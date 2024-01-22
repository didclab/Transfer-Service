package org.onedatashare.transferservice.odstransferservice.pools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.stereotype.Service;

import java.util.HashMap;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.PARALLEL_POOL_PREFIX;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.STEP_POOL_PREFIX;

@Service("threadPool")
@Profile("virtual")
public class ThreadPoolManagerVirtual implements ThreadPoolContract {

    HashMap<String, SimpleAsyncTaskExecutor> executorHashmap;

    Logger logger = LoggerFactory.getLogger(ThreadPoolManagerVirtual.class);

    public ThreadPoolManagerVirtual() {
        this.executorHashmap = new HashMap<>();
    }


    @Override
    public SimpleAsyncTaskExecutor createExecutor(int threadCount, String prefix) {
        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor();
        executor.setThreadNamePrefix(prefix);
        executor.setVirtualThreads(true);
        executor.setConcurrencyLimit(threadCount);
        if (this.executorHashmap == null) {
            this.executorHashmap = new HashMap<>();
        }
        logger.info("Created a SimpleAsyncTaskExecutor: Prefix:{} with size:{}", prefix, threadCount);
        this.executorHashmap.put(prefix, executor);
        return executor;
    }

    /**
     * @param concurrency
     * @param parallel
     */
    public void applyOptimizer(int concurrency, int parallel) {
        SimpleAsyncTaskExecutor stepPool = this.executorHashmap.get(STEP_POOL_PREFIX);
        if (stepPool != null) {
            if (concurrency > 0 && concurrency != stepPool.getConcurrencyLimit()) {
                stepPool.setConcurrencyLimit(concurrency);
                logger.info("Set {} pool size to {}", stepPool.getThreadNamePrefix(), concurrency);
            }
        }
        for (String key : this.executorHashmap.keySet()) {
            if (key.contains(PARALLEL_POOL_PREFIX)) {
                SimpleAsyncTaskExecutor parallelPool = this.executorHashmap.get(key);
                if (parallelPool != null) {
                    if (parallel > 0 && parallel != parallelPool.getConcurrencyLimit()) {
                        parallelPool.setConcurrencyLimit(parallel);
                        logger.info("Set {} pool size to {}", parallelPool.getThreadNamePrefix(), parallel);
                    }
                }
            }
        }
    }

    @Override
    public void clearPools() {
        for (String key : this.executorHashmap.keySet()) {
            SimpleAsyncTaskExecutor pool = this.executorHashmap.get(key);
            pool.close();
            logger.info("Shutting SimpleAsyncTaskExec down {}", pool.getThreadNamePrefix());
        }
        this.executorHashmap.clear();
        logger.info("Cleared all thread pools");
    }

    @Override
    public SimpleAsyncTaskExecutor stepPool(int threadCount) {
        SimpleAsyncTaskExecutor te = this.executorHashmap.get(STEP_POOL_PREFIX);
        if (te == null) {
            return this.createExecutor(threadCount, STEP_POOL_PREFIX);
        }
        return te;
    }

    @Override
    public SimpleAsyncTaskExecutor parallelPool(int threadCount, String filePath) {
        SimpleAsyncTaskExecutor te = this.executorHashmap.get(PARALLEL_POOL_PREFIX + filePath);
        if (te == null) {
            te = this.createExecutor(threadCount, PARALLEL_POOL_PREFIX + filePath);
        }
        return te;
    }

    public int concurrencyCount() {
        SimpleAsyncTaskExecutor threadPoolManager = this.executorHashmap.get(STEP_POOL_PREFIX);
        if (threadPoolManager == null) {
            return 0;
        }
        return threadPoolManager.getConcurrencyLimit();
    }

    public int parallelismCount() {
        for (String key : this.executorHashmap.keySet()) {
            if (key.contains(PARALLEL_POOL_PREFIX)) {
                SimpleAsyncTaskExecutor executor = this.executorHashmap.get(key);
                if (executor != null) {
                    return executor.getConcurrencyLimit();
                }
            }
        }
        return 0;
    }
}
