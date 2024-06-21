package org.onedatashare.transferservice.odstransferservice.pools;

import org.springframework.core.task.TaskExecutor;

public interface ThreadPoolContract {
    public TaskExecutor createExecutor(int threadCount, String prefix);
    public void applyOptimizer(int concurrency, int parallelism);
    public void clearPools();
    public int concurrencyCount();
    public int parallelismCount();
    public TaskExecutor stepPool(int threadCount);
    public TaskExecutor parallelPool(int threadCount, String filePath);
}
