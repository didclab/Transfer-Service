package org.onedatashare.transferservice.odstransferservice.pools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.util.HashMap;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.PARALLEL_POOL_PREFIX;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.STEP_POOL_PREFIX;

@Service("threadPool")
@Profile("platform")
public class ThreadPoolManagerPlatform implements ThreadPoolContract {
    HashMap<String, ThreadPoolTaskExecutor> platformThreadMap;
    Logger logger = LoggerFactory.getLogger(ThreadPoolManagerPlatform.class);

    public ThreadPoolManagerPlatform() {
        this.platformThreadMap = new HashMap<>();
    }

    @Override
    public ThreadPoolTaskExecutor createExecutor(int threadCount, String prefix) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setAllowCoreThreadTimeOut(false);
        executor.setCorePoolSize(threadCount);
        executor.setThreadNamePrefix(prefix);
        executor.initialize();
        if (this.platformThreadMap == null) {
            this.platformThreadMap = new HashMap<>();
        }
        logger.info("Created ThreadPoolTaskExecutor: Prefix:{} with size:{}", prefix, threadCount);
        this.platformThreadMap.put(prefix, executor);
        return executor;
    }

    @Override
    public void applyOptimizer(int concurrency, int parallelism) {
        for (String key : this.platformThreadMap.keySet()) {
            ThreadPoolTaskExecutor pool = this.platformThreadMap.get(key);
            if (key.contains(STEP_POOL_PREFIX)) {
                if (concurrency > 0 && concurrency != pool.getPoolSize()) {
                    pool.setCorePoolSize(concurrency);
                    logger.info("Set {} pool size to {}", pool.getThreadNamePrefix(), concurrency);
                }
            }
            if (key.contains(PARALLEL_POOL_PREFIX)) {
                logger.info("Changing {} pool size from {} to {}", pool.getThreadNamePrefix(), pool.getPoolSize(), parallelism);
                if (parallelism > 0 && parallelism != pool.getPoolSize()) {
                    pool.setCorePoolSize(parallelism);
                    logger.info("Set {} pool size to {}", pool.getThreadNamePrefix(), parallelism);
                }
            }
        }
    }

    @Override
    public void clearPools() {
        for (String key : this.platformThreadMap.keySet()) {
            ThreadPoolTaskExecutor pe = this.platformThreadMap.get(key);
            pe.shutdown();
        }
        this.platformThreadMap.clear();
    }

    @Override
    public int concurrencyCount() {
        ThreadPoolTaskExecutor pe = this.platformThreadMap.get(STEP_POOL_PREFIX);
        if (pe == null) {
            return 0;
        }
        return pe.getCorePoolSize();
    }

    @Override
    public int parallelismCount() {
        for (String key : this.platformThreadMap.keySet()) {
            if (key.contains(PARALLEL_POOL_PREFIX)) {
                ThreadPoolTaskExecutor threadPoolManager = this.platformThreadMap.get(PARALLEL_POOL_PREFIX);
                if (threadPoolManager != null) {
                    return threadPoolManager.getCorePoolSize();
                }
            }
        }
        return 0;
    }

    @Override
    public ThreadPoolTaskExecutor stepPool(int threadCount) {
        ThreadPoolTaskExecutor te = this.platformThreadMap.get(STEP_POOL_PREFIX);
        if (te == null) {
            return this.createExecutor(threadCount, STEP_POOL_PREFIX);
        }
        return te;
    }

    @Override
    public ThreadPoolTaskExecutor parallelPool(int threadCount, String filePath) {
        ThreadPoolTaskExecutor te = this.platformThreadMap.get(PARALLEL_POOL_PREFIX + filePath);
        if (te == null) {
            te = this.createExecutor(threadCount, PARALLEL_POOL_PREFIX + filePath);
        }
        return te;
    }
}
