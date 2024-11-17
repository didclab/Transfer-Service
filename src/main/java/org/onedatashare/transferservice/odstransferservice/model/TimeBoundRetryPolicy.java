package org.onedatashare.transferservice.odstransferservice.model;

import org.springframework.retry.RetryContext;
import org.springframework.retry.policy.SimpleRetryPolicy;

public class TimeBoundRetryPolicy extends SimpleRetryPolicy {

    private long maxRetryTimeMillis;
    private long startTimeMillis;

    public TimeBoundRetryPolicy(long maxRetryTimeMillis) {
        this.maxRetryTimeMillis = maxRetryTimeMillis;
    }

    @Override
    public boolean canRetry(RetryContext context) {
        if(System.currentTimeMillis() - startTimeMillis >= maxRetryTimeMillis) {
            return false;
        }
        return super.canRetry(context);
    }

    @Override
    public RetryContext open(RetryContext retryContext) {
        super.open(retryContext);
        this.startTimeMillis = System.currentTimeMillis();
        return retryContext;
    }
}
