package org.onedatashare.transferservice.odstransferservice.model.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EntityInfo {
    private String id;
    private String path;
    private long size;
}
