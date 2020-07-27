package org.onedatashare.transferservice.odstransferservice.service.abstracClass;

import org.onedatashare.transferservice.odstransferservice.model.Slice;

public abstract class Tap {
    Slice slice;
    public abstract Slice openTap(int sliceSize);
    public abstract Slice getSlice();
}
