package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Getter;
import lombok.Setter;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.PriorityQueue;

/**
 * This class is used to Buffer all of the data from a small file. So any file less than 20MB according to the box api
 */
@Getter
@Setter
public class BoxSmallFileUpload {
    private PriorityQueue<DataChunk> dataChunkPriorityQueue;

    public BoxSmallFileUpload(){
        this.dataChunkPriorityQueue = new PriorityQueue<DataChunk>(new DataChunkComparator());
    }

    public void addAllChunks(List<? extends DataChunk> chunks){
        this.dataChunkPriorityQueue.addAll(chunks);
    }

    public InputStream condenseListToOneStream(long size){
        byte[] data = new byte[Long.valueOf(size).intValue()];//we know this file will always be <= 20MB
        ByteBuffer buffer = ByteBuffer.wrap(data);
        for(DataChunk chunk : this.dataChunkPriorityQueue){
            buffer.put(chunk.getData());
        }
        this.dataChunkPriorityQueue.clear();
        return new ByteArrayInputStream(buffer.array());
    }
}
