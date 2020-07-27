package org.onedatashare.transferservice.odstransferservice.service.abstracClass;

import lombok.NoArgsConstructor;
import org.onedatashare.transferservice.odstransferservice.model.Slice;


@NoArgsConstructor
public abstract class Drain {
    Slice slice;
    public abstract void drain(Slice slice) throws Exception;
    public abstract void finish() throws Exception;
    public abstract Slice getSlice();
}
