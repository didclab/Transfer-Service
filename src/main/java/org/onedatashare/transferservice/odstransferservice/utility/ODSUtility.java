package org.onedatashare.transferservice.odstransferservice.utility;

import com.amazonaws.regions.Regions;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;

public class ODSUtility {

    public static DataChunk makeChunk(int size, byte[] data, int startPosition, String fileName) {
        DataChunk dataChunk = new DataChunk();
        dataChunk.setStartPosition(startPosition);
        dataChunk.setFileName(fileName);
        dataChunk.setData(data);
        dataChunk.setSize(size);
        return dataChunk;
    }

}
