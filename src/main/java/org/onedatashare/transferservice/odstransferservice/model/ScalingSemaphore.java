package org.onedatashare.transferservice.odstransferservice.model;

import java.util.concurrent.Semaphore;

public class ScalingSemaphore extends Semaphore {


    public ScalingSemaphore(int permits) {
        super(permits);
    }

    @Override
    public void reducePermits(int reduction) {
        super.reducePermits(reduction);
    }

}
