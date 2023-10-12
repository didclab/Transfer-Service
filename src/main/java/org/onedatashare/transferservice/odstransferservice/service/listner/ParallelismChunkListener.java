package org.onedatashare.transferservice.odstransferservice.service.listner;

import lombok.Getter;
import org.onedatashare.transferservice.odstransferservice.model.ScalingSemaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.stereotype.Service;


@Service
public class ParallelismChunkListener implements ChunkListener {

    ScalingSemaphore semaphore;
    @Getter
    private int parallelism;
    Logger logger = LoggerFactory.getLogger(ParallelismChunkListener.class);

    public ParallelismChunkListener() {
        this.semaphore = new ScalingSemaphore(1);
    }

    public void changeParallelism(int nextParallelism) {
        if(nextParallelism > this.parallelism){
            int diff = nextParallelism - this.parallelism;
            this.semaphore.release(diff);
        }else{
            int diff = this.parallelism - nextParallelism;
            try {
                this.semaphore.acquire(diff);
            } catch (InterruptedException e) {
                logger.error("Failed to acquire locks: " + diff);
            }
        }
        this.parallelism = nextParallelism;
        logger.info("Parallelism {} became parallelism {}", this.parallelism, nextParallelism);
    }

    public void beforeChunk(ChunkContext context) {
        while (!this.semaphore.tryAcquire()) {

        }
        logger.info("Thread: {} acquired a single permit", Thread.currentThread());


    }

    public void afterChunk(ChunkContext context) {
        this.semaphore.release();
        logger.info("Thread: {} releasing a single permit", Thread.currentThread());
    }

    public void afterChunkError(ChunkContext context) {
    }
}
