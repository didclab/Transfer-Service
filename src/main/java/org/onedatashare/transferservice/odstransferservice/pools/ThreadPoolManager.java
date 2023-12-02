package org.onedatashare.transferservice.odstransferservice.pools;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.util.HashMap;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

@Service
public class ThreadPoolManager {

    @Getter
    HashMap<String, SimpleAsyncTaskExecutor> executorHashmap;
    HashMap<String, ThreadPoolTaskExecutor> platformThreadMap;

    Logger logger = LoggerFactory.getLogger(ThreadPoolManager.class);
    public ThreadPoolManager() {
        this.executorHashmap = new HashMap<>();
        this.platformThreadMap = new HashMap<>();
    }

    public ThreadPoolTaskExecutor createPlatformThreads(int corePoolSize, String prefix){
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
//        executor.setQueueCapacity(1);
        executor.setAllowCoreThreadTimeOut(false);
        executor.setCorePoolSize(corePoolSize);
//        executor.setMaxPoolSize(corePoolSize);
        executor.setThreadNamePrefix(prefix);
        executor.initialize();
        if (this.executorHashmap == null) {
            this.executorHashmap = new HashMap<>();
        }
        logger.info("Created ThreadPoolTaskExecutor: Prefix:{} with size:{}", prefix, corePoolSize);
        this.platformThreadMap.put(prefix, executor);
        return executor;
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
                logger.info("Changing {} pool size from {} to {}", pool.getThreadNamePrefix(), pool.getConcurrencyLimit(), concurrency);
                if (concurrency > 0 && concurrency != pool.getConcurrencyLimit()) {
//                    pool.setMaxPoolSize(concurrency);
                    pool.setConcurrencyLimit(concurrency);
                    logger.info("Set {} pool size to {}", pool.getThreadNamePrefix(), concurrency);
                }
            }
        }
        for(String key: this.platformThreadMap.keySet()){
            ThreadPoolTaskExecutor pool = this.platformThreadMap.get(key);
            if (key.contains(PARALLEL_POOL_PREFIX)) {
                logger.info("Changing {} pool size from {} to {}", pool.getThreadNamePrefix(), pool.getCorePoolSize(), parallel);
                if (parallel > 0 && parallel != pool.getPoolSize()) {
//                    pool.setMaxPoolSize(parallel);
                    pool.setCorePoolSize(parallel);
                }
            }
        }
    }

    public void clearJobPool() {
        for(String key : this.platformThreadMap.keySet()){
            ThreadPoolTaskExecutor pool = this.platformThreadMap.get(key);
            pool.shutdown();
            logger.info("Shutting ThreadPoolTaskExecutor down {}", pool.getThreadNamePrefix());
        }
        for(String key: this.executorHashmap.keySet()){
            SimpleAsyncTaskExecutor pool = this.executorHashmap.get(key);
            pool.close();
            logger.info("Shutting SimpleAsyncTaskExec down {}", pool.getThreadNamePrefix());
        }
        this.executorHashmap.clear();
        this.platformThreadMap.clear();
        logger.info("Cleared all thread pools");
    }

    public SimpleAsyncTaskExecutor sequentialThreadPool() {
        return this.createVirtualThreadExecutor(1, SEQUENTIAL_POOL_PREFIX);
    }

    public SimpleAsyncTaskExecutor stepTaskExecutor(int threadCount) {
        SimpleAsyncTaskExecutor te = this.executorHashmap.get(STEP_POOL_PREFIX);
        if (te == null) {
            return this.createVirtualThreadExecutor(threadCount, STEP_POOL_PREFIX);
        }
        return te;
    }

    public ThreadPoolTaskExecutor stepTaskExecutorPlatform(int threadCount){
        ThreadPoolTaskExecutor te = this.platformThreadMap.get(STEP_POOL_PREFIX);
        if(te == null){
            return this.createPlatformThreads(threadCount, STEP_POOL_PREFIX);
        }
        return te;
    }

    public SimpleAsyncTaskExecutor parallelThreadPool(int threadCount) {
        SimpleAsyncTaskExecutor te = this.executorHashmap.get(PARALLEL_POOL_PREFIX);
        if (te == null) {
            te = this.createVirtualThreadExecutor(threadCount, PARALLEL_POOL_PREFIX);
        }
        return te;
    }

    public ThreadPoolTaskExecutor parallelThreadPool(int threadCount, String fileName) {
        return this.createPlatformThreads(threadCount, new StringBuilder().append(fileName).append("-").append(PARALLEL_POOL_PREFIX).toString());
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
        for (String key : this.platformThreadMap.keySet()) {
            if (key.contains(PARALLEL_POOL_PREFIX)) {
                parallelism = this.platformThreadMap.get(key).getCorePoolSize();
                if (parallelism > 0) {
                    return parallelism;
                }
            }
        }
        return parallelism;
    }

}
