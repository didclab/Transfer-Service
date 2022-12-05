package org.onedatashare.transferservice.odstransferservice.pools;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.micrometer.influx.InfluxMeterRegistry;
import lombok.Getter;
import org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

@Service
public class ThreadPoolManager {

    @Getter
    HashMap<String, ThreadPoolTaskExecutor> executorHashmap;

    Logger logger = LoggerFactory.getLogger(ThreadPoolManager.class);

    @Autowired
    InfluxMeterRegistry registry;

    @PostConstruct
    public void createMap() {
        this.executorHashmap = new HashMap<>();
        logger.info("creating executor hashmap");
    }

    public ThreadPoolTaskExecutor createThreadPool(int corePoolSize, String prefix) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(corePoolSize);
        executor.setThreadNamePrefix(prefix);
        executor.setAllowCoreThreadTimeOut(true);
        executor.initialize();
        if (this.executorHashmap == null) {
            this.executorHashmap = new HashMap<>();
        }
        Iterable<Tag> tags = List.of();
        ExecutorServiceMetrics serviceMetrics = new ExecutorServiceMetrics(executor.getThreadPoolExecutor(), executor.getThreadNamePrefix(), tags);
        serviceMetrics.bindTo(registry);

        this.executorHashmap.put(prefix, executor);
        return executor;
    }

    /**
     * @param concurrency
     * @param parallel
     */
    public void applyOptimizer(int concurrency, int parallel) {
        for (String key : this.executorHashmap.keySet()) {
            ThreadPoolTaskExecutor pool = this.executorHashmap.get(key);
            if (key.contains(STEP_POOL_PREFIX)) {
                logger.info("Changing {} pool size from {} to {}", pool.getThreadNamePrefix(), pool.getPoolSize(), concurrency);
                if (concurrency > 0) {
                    pool.setCorePoolSize(concurrency);
                    logger.info("Set {} pool size to {}", pool.getThreadNamePrefix(), concurrency);
                }
            }
            if (key.contains(PARALLEL_POOL_PREFIX)) {
                logger.info("Changing {} pool size from {} to {}", pool.getThreadNamePrefix(), pool.getPoolSize(), parallel);
                if (parallel > 0) {
                    pool.setCorePoolSize(parallel);
                }
            }
        }
    }

    public void clearJobPool() {
        Iterator<Map.Entry<String, ThreadPoolTaskExecutor>> iterator = this.executorHashmap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, ThreadPoolTaskExecutor> cur = iterator.next();
            ThreadPoolTaskExecutor pool = cur.getValue();
            String key = cur.getKey();
            if (key.contains(STEP_POOL_PREFIX) || key.contains(PARALLEL_POOL_PREFIX)) {
                pool.shutdown();
                iterator.remove();
            }
        }
    }

    public ThreadPoolTaskExecutor sequentialThreadPool() {
        return this.createThreadPool(1, SEQUENTIAL_POOL_PREFIX);
    }

    public ThreadPoolTaskExecutor stepTaskExecutor(int threadCount) {
        return this.createThreadPool(threadCount, STEP_POOL_PREFIX);
    }

    public ThreadPoolTaskExecutor parallelThreadPool(int threadCount, String fileName) {
        return this.createThreadPool(threadCount, new StringBuilder().append(fileName).append("-").append(PARALLEL_POOL_PREFIX).toString());
    }

    public Integer concurrencyCount() {
        ThreadPoolTaskExecutor threadPoolManager = this.executorHashmap.get(STEP_POOL_PREFIX);
        if (threadPoolManager == null) {
            return 0;
        }
        return threadPoolManager.getCorePoolSize();
    }

    public Integer parallelismCount() {
        int parallelism = 0;
        for (String key : this.executorHashmap.keySet()) {
            if (key.contains(PARALLEL_POOL_PREFIX)) {
                parallelism = this.executorHashmap.get(key).getCorePoolSize();
                break;
            }
        }
        return parallelism;
    }

}
