package org.onedatashare.transferservice.odstransferservice.pools;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

@Service
public class ThreadPoolManager {

    @Getter
    HashMap<String, SimpleAsyncTaskExecutor> executorHashmap;

    Logger logger = LoggerFactory.getLogger(ThreadPoolManager.class);

    @PostConstruct
    public void createMap() {
        this.executorHashmap = new HashMap<>();
    }

    public SimpleAsyncTaskExecutor createThreadPool(int corePoolSize, String prefix) {

        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor();
        executor.setThreadNamePrefix(prefix);
        executor.setVirtualThreads(true);
        executor.setConcurrencyLimit(corePoolSize);
        if (this.executorHashmap == null) {
            this.executorHashmap = new HashMap<>();
        }
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
                logger.info("Changing {} pool size from {} to {}", pool.getThreadNamePrefix(), pool.getConcurrencyLimit(), concurrency);
                if (concurrency > 0 && concurrency != pool.getConcurrencyLimit()) {
//                    pool.setMaxPoolSize(concurrency);
                    pool.setConcurrencyLimit(concurrency);
                    logger.info("Set {} pool size to {}", pool.getThreadNamePrefix(), concurrency);
                }
            }
            if (key.contains(PARALLEL_POOL_PREFIX)) {
                logger.info("Changing {} pool size from {} to {}", pool.getThreadNamePrefix(), pool.getConcurrencyLimit(), parallel);
                if (parallel > 0 && parallel != pool.getConcurrencyLimit()) {
//                    pool.setMaxPoolSize(parallel);
                    pool.setConcurrencyLimit(parallel);
                }
            }
        }
    }

    public void clearJobPool() {
        Iterator<Map.Entry<String, SimpleAsyncTaskExecutor>> iterator = this.executorHashmap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, SimpleAsyncTaskExecutor> cur = iterator.next();
            SimpleAsyncTaskExecutor pool = cur.getValue();
            String key = cur.getKey();
            if (key.contains(STEP_POOL_PREFIX) || key.contains(PARALLEL_POOL_PREFIX)) {
                pool.close();
                iterator.remove();
            }
        }
    }

    public SimpleAsyncTaskExecutor sequentialThreadPool() {
        return this.createThreadPool(1, SEQUENTIAL_POOL_PREFIX);
    }

    public SimpleAsyncTaskExecutor stepTaskExecutor(int threadCount) {
        SimpleAsyncTaskExecutor te = this.executorHashmap.get(STEP_POOL_PREFIX);
        if (te == null) {
            return this.createThreadPool(threadCount, STEP_POOL_PREFIX);
        }
        return te;
    }

    public SimpleAsyncTaskExecutor parallelThreadPool(int threadCount) {
        SimpleAsyncTaskExecutor te = this.executorHashmap.get(PARALLEL_POOL_PREFIX);
        if (te == null) {
            te = this.createThreadPool(threadCount, PARALLEL_POOL_PREFIX);
        }
        return te;
    }

    public SimpleAsyncTaskExecutor parallelThreadPool(int threadCount, String fileName) {
        return this.createThreadPool(threadCount, new StringBuilder().append(fileName).append("-").append(PARALLEL_POOL_PREFIX).toString());
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
