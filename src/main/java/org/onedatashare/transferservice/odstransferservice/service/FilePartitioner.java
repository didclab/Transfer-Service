package org.onedatashare.transferservice.odstransferservice.service;

import org.onedatashare.transferservice.odstransferservice.model.FilePart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.SIXTYFOUR_KB;
public class FilePartitioner {
    Logger logger = LoggerFactory.getLogger(FilePartitioner.class);
    ConcurrentLinkedQueue<FilePart> queue;
    public int chunkSize;

    public FilePartitioner(){
        this.queue = new ConcurrentLinkedQueue<>();
        this.chunkSize = SIXTYFOUR_KB;
    }

    public FilePartitioner(int chunkSize){
        this.queue = new ConcurrentLinkedQueue<>();
        this.chunkSize = chunkSize;
    }

    public FilePart nextPart(){
        return this.queue.poll();
    }

    public void returnPart(FilePart part) {
        this.queue.add(part);
    }

    /**
     * Returning -1 means an error occured.
     * @param totalSize
     * @param fileName
     * @return
     */
    public int createParts(long totalSize, String fileName){
        if(totalSize < 1) return -1;
        if(totalSize < this.chunkSize){
            FilePart part = new FilePart();
            part.setLastChunk(true);
            part.setFileName(fileName);
            part.setStart(0);
            part.setEnd(totalSize);
            part.setSize(Long.valueOf(totalSize).intValue());
            queue.add(part);
        }else{
            long startPosition = 0;
            long chunksOfChunksKB = Math.floorDiv(totalSize, this.chunkSize);
            for(long i = 0; i < chunksOfChunksKB; i++){
                FilePart part = new FilePart();
                part.setLastChunk(false);
                part.setFileName(fileName);
                part.setPartIdx(i);
                part.setSize(this.chunkSize);
                part.setStart(startPosition);
                startPosition+=this.chunkSize-1;
                part.setEnd(startPosition);
                startPosition++;
                this.queue.add(part);
            }
            FilePart lastChunk = new FilePart();
            lastChunk.setStart(startPosition);
            lastChunk.setFileName(fileName);
            lastChunk.setLastChunk(true);
            lastChunk.setSize(Long.valueOf(totalSize-startPosition).intValue());
            lastChunk.setPartIdx(chunksOfChunksKB);
            lastChunk.setEnd(totalSize);
            queue.add(lastChunk);
            logger.info(lastChunk.toString());
        }
        logger.info("The total size of the queue after parsing file: " + fileName +" " +queue.size());
        return queue.size();
    }

}
