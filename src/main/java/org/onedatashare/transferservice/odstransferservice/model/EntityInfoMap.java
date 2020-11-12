package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;


public class EntityInfoMap {
    @Getter
    @Setter
    static Map<String,Long> hm = new HashMap<>();

    //TODO: Must remove these and find way to pass to reader and writer
    public static String sPass = "";
    public static String dPass = "";
}
