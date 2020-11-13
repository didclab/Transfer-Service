package org.onedatashare.transferservice.odstransferservice.service;

import org.junit.jupiter.api.Test;
import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.SIXTYFOUR_KB;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

class FilePartitionerTest {

    FilePartitioner testObj = new FilePartitioner();

    @Test
    void testPrepareQueue() {
        int queue = testObj.createParts(0, "test");
        assertEquals(-1, queue);
    }

    @Test
    public void testPrepareQueueOnePartitions(){
        int size = testObj.createParts(64000, "test");
        assertEquals(1, size);
    }

    @Test
    public void testPrepareQueuetwoPartitions(){
        int size = testObj.createParts(64002, "test");
        assertEquals(2,size);
        FilePart one = testObj.nextPart();
        FilePart two = testObj.nextPart();
        assertEquals(one.getSize(), SIXTYFOUR_KB);
        assertEquals(two.getSize(), 2);
    }

    @Test
    public void testPrepareQueueSixFourEightPartitions(){
        assertEquals(648, testObj.createParts(41470883, "test"));
    }
}