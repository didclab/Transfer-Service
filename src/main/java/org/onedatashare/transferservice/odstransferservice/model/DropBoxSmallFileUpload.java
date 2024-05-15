package org.onedatashare.transferservice.odstransferservice.model;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.PriorityQueue;

import lombok.Getter;
import lombok.Setter;

/**
 * This class is used to Buffer all of the data from a small file. So any file less than 20MB according to the box api
 */
@Getter
@Setter
public class DropBoxSmallFileUpload {
    private PriorityQueue<DataChunk> dataChunkPriorityQueue;

    public DropBoxSmallFileUpload(){
        this.dataChunkPriorityQueue = new PriorityQueue<DataChunk>(new DataChunkComparator());
    }

    public void addAllChunks(List<? extends DataChunk> chunks){
        this.dataChunkPriorityQueue.addAll(chunks);
    }

    public InputStream condenseListToOneStream(){
        int totalLength = this.dataChunkPriorityQueue.stream().mapToInt(byteArray -> byteArray.getData().length).sum();
        byte[] combinedBytes = new byte[totalLength];

        int currentIndex = 0;
        for (DataChunk chunk : dataChunkPriorityQueue) {
            byte[] byteArray = chunk.getData();
            System.arraycopy(byteArray, 0, combinedBytes, currentIndex, byteArray.length);
            currentIndex += byteArray.length;
        }

        return new ByteArrayInputStream(combinedBytes);
    }
}
