package org.onedatashare.transferservice.odstransferservice.service.listner;

import lombok.Getter;
import lombok.Setter;
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
    Semaphore semaphore;

    @Getter
    @Setter
    int concurrency;

    public ConcurrencyStepListener() {
        this.concurrency = 32;
        this.semaphore = new Semaphore(concurrency);
    }

    public void changeConcurrency(int nextConcurrency) {
        if (nextConcurrency > this.concurrency) {
            int diff = nextConcurrency - this.concurrency;
            this.semaphore.release(diff);
        } else {
            int diff = this.concurrency - nextConcurrency;
            this.semaphore.acquireUninterruptibly(diff);
        }
        logger.info("Concurrency changed from: {} to {}", this.concurrency, nextConcurrency);
        this.concurrency = nextConcurrency;
    }

    public void beforeStep(StepExecution stepExecution) {
        logger.info("Thread: {} trying to acquire a single Concurrent permit", Thread.currentThread());
        this.semaphore.acquireUninterruptibly();
        logger.info("Thread: {} acquired Concurrent permit StepName={}", Thread.currentThread(), stepExecution.getStepName());
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        this.semaphore.release();
        logger.info("Thread: {} releasing Concurrent permit StepName={}", Thread.currentThread(), stepExecution.getStepName());
        return null;
    }
}
