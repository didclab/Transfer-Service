package org.onedatashare.transferservice.odstransferservice.service.listner;

import lombok.Getter;
import lombok.Setter;
import org.onedatashare.transferservice.odstransferservice.model.ScalingSemaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.Semaphore;

@Service
public class ConcurrencyStepListener implements StepExecutionListener {

    Logger logger = LoggerFactory.getLogger(ConcurrencyStepListener.class);
    ScalingSemaphore semaphore;
    @Getter
    @Setter
    int concurrency;

    public ConcurrencyStepListener() {
        this.concurrency = 1;
        this.semaphore = new ScalingSemaphore(concurrency);
    }

    public void changeConcurrency(int nextConcurrency) {
        if(nextConcurrency > this.concurrency){
            int diff = nextConcurrency - this.concurrency;
            this.semaphore.release(diff);
        }else{
            int diff = this.concurrency - nextConcurrency;
            try {
                this.semaphore.acquire(diff);
            } catch (InterruptedException e) {
                logger.error("Failed to acquire locks: " + diff);
            }
        }
        this.concurrency = nextConcurrency;
        logger.info("Concurrency {} became Concurrency {}", this.concurrency, nextConcurrency);
    }

    public void beforeStep(StepExecution stepExecution) {
        logger.info("Thread: {} trying to acquire a single permit", Thread.currentThread());
        while (!this.semaphore.tryAcquire()) {
            try {
                Thread.sleep(0, 1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        logger.info("Thread: {} acquired a single permit", Thread.currentThread());
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        this.semaphore.release();
        logger.info("Thread: {} releasing a single permit", Thread.currentThread());
        return null;
    }
}
