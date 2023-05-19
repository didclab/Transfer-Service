package org.onedatashare.transferservice.odstransferservice.model;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.PriorityQueue;

public class FileBuffer {
    private PriorityQueue<DataChunk> dataChunkPriorityQueue;

    public FileBuffer(){
        this.dataChunkPriorityQueue = new PriorityQueue<DataChunk>(new DataChunkComparator());
    }
    public void addChunk(DataChunk chunk){
        this.dataChunkPriorityQueue.add(chunk);
    }
    public void addAllChunks(List<? extends DataChunk> chunks){
        this.dataChunkPriorityQueue.addAll(chunks);
    }

    public InputStream condenseListToOneStream(long size){
        byte[] data = new byte[Long.valueOf(size).intValue()];
        ByteBuffer buffer = ByteBuffer.wrap(data);
        for(DataChunk chunk : this.dataChunkPriorityQueue){
            buffer.put(chunk.getData());
        }
        return new ByteArrayInputStream(buffer.array());
    }

    public void clear(){
        this.dataChunkPriorityQueue.clear();
    }
}
