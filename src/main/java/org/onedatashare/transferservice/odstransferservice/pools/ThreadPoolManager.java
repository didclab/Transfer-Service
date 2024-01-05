package org.onedatashare.transferservice.odstransferservice.pools;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.stereotype.Service;

import java.util.HashMap;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.PARALLEL_POOL_PREFIX;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.STEP_POOL_PREFIX;

@Service
public class ThreadPoolManager {

    @Getter
    HashMap<String, SimpleAsyncTaskExecutor> executorHashmap;

    Logger logger = LoggerFactory.getLogger(ThreadPoolManager.class);

    public ThreadPoolManager() {
        this.executorHashmap = new HashMap<>();
    }

    public SimpleAsyncTaskExecutor createVirtualThreadExecutor(int corePoolSize, String prefix) {
        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor();
        executor.setThreadNamePrefix(prefix);
        executor.setVirtualThreads(true);
        executor.setConcurrencyLimit(corePoolSize);
        if (this.executorHashmap == null) {
            this.executorHashmap = new HashMap<>();
        }
        logger.info("Created a SimpleAsyncTaskExecutor: Prefix:{} with size:{}", prefix, corePoolSize);
        this.executorHashmap.put(prefix, executor);
        return executor;
    }

    /**
     * @param concurrency
     * @param parallel
     */
    public void applyOptimizer(int concurrency, int parallel) {
        for (String key : this.executorHashmap.keySet()) {
            SimpleAsyncTaskExecutor pool = this.executorHashmap.get(key);
            if (key.contains(STEP_POOL_PREFIX)) {
                if (concurrency > 0 && concurrency != pool.getConcurrencyLimit()) {
                    pool.setConcurrencyLimit(concurrency);
                    logger.info("Set {} pool size to {}", pool.getThreadNamePrefix(), concurrency);
                }
            }
            if (key.contains(PARALLEL_POOL_PREFIX)) {
                if (parallel > 0 && parallel != pool.getConcurrencyLimit()) {
                    pool.setConcurrencyLimit(parallel);
                    logger.info("Set {} pool size to {}", pool.getThreadNamePrefix(), parallel);
                }
            }
        }

    }

    public void clearJobPool() {
        for (String key : this.executorHashmap.keySet()) {
            SimpleAsyncTaskExecutor pool = this.executorHashmap.get(key);
            pool.close();
            logger.info("Shutting SimpleAsyncTaskExec down {}", pool.getThreadNamePrefix());
        }
        this.executorHashmap.clear();
        logger.info("Cleared all thread pools");
    }

    public SimpleAsyncTaskExecutor stepTaskExecutorVirtual(int threadCount) {
        SimpleAsyncTaskExecutor te = this.executorHashmap.get(STEP_POOL_PREFIX);
        if (te == null) {
            return this.createVirtualThreadExecutor(threadCount, STEP_POOL_PREFIX);
        }
        return te;
    }

    public SimpleAsyncTaskExecutor parallelThreadPoolVirtual(int threadCount, String fileName) {
        SimpleAsyncTaskExecutor te = this.executorHashmap.get(PARALLEL_POOL_PREFIX + fileName);
        if (te == null) {
            te = this.createVirtualThreadExecutor(threadCount, PARALLEL_POOL_PREFIX + fileName);
        }
        return te;
    }

    public Integer concurrencyCount() {
        SimpleAsyncTaskExecutor threadPoolManager = this.executorHashmap.get(STEP_POOL_PREFIX);
        if (threadPoolManager == null) {
            return 0;
        }
        return threadPoolManager.getConcurrencyLimit();
    }

    public Integer parallelismCount() {
        int parallelism = 0;
        for (String key : this.executorHashmap.keySet()) {
            if (key.contains(PARALLEL_POOL_PREFIX)) {
                parallelism = this.executorHashmap.get(key).getConcurrencyLimit();
                if (parallelism > 0) {
                    return parallelism;
                }
            }
        }
        return parallelism;
    }

}
